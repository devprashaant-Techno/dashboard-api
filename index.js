require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.static('public'));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('supabase')
    ? { rejectUnauthorized: false }
    : false
});

// ── AUTO-DETECT tables and columns ──────────────────────────────────
async function getTables(client) {
  const res = await client.query(`
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
  `);
  return res.rows.map(r => r.table_name);
}

async function getColumns(client, table) {
  const res = await client.query(`
    SELECT column_name, data_type FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1
  `, [table]);
  return res.rows;
}

// ── FIND columns by likely name patterns ───────────────────────────
function findCol(cols, patterns) {
  for (const p of patterns) {
    const found = cols.find(c => c.column_name.toLowerCase().includes(p.toLowerCase()));
    if (found) return found.column_name;
  }
  return null;
}

// ── MAIN METRICS ENDPOINT ───────────────────────────────────────────
app.get('/metrics', async (req, res) => {
  const client = await pool.connect();
  try {
    const tables = await getTables(client);
    const distributors = {};
    let period = '';

    // Try to find the main orders/sales table
    const salesTable = tables.find(t =>
      ['orders','sales','transactions','invoices','order_details'].some(k => t.toLowerCase().includes(k))
    ) || tables[0];

    if (!salesTable) {
      return res.json({ distributors: {}, period: 'No data', lastUpdated: new Date() });
    }

    const cols = await getColumns(client, salesTable);
    const colNames = cols.map(c => c.column_name);

    // Auto-detect key columns
    const distCol    = findCol(cols, ['distributor','dist_name','dealer','partner','company']);
    const revenueCol = findCol(cols, ['revenue','amount','total','value','gmv','sale_amount','net_amount']);
    const ordersCol  = findCol(cols, ['orders','order_count','order_id','invoice']);
    const qtyCol     = findCol(cols, ['qty','quantity','units','pieces','sold']);
    const dateCol    = findCol(cols, ['date','month','created_at','order_date','invoice_date','period']);
    const categoryCol= findCol(cols, ['category','gender','segment','type','dept']);
    const styleCol   = findCol(cols, ['style','style_name','product','article','sku','item']);
    const onbCol     = findCol(cols, ['onboard','onboarded','new_retailer','activated']);
    const churnCol   = findCol(cols, ['churn','churned','inactive','deactivated']);
    const kycCol     = findCol(cols, ['kyc','kyc_done','verified','kyc_status']);

    if (!distCol) {
      return res.json({ distributors: {}, period: 'No distributor column found', lastUpdated: new Date() });
    }

    // ── Get all distributors ──────────────────────────────────────
    const distList = await client.query(
      `SELECT DISTINCT "${distCol}" FROM "${salesTable}" WHERE "${distCol}" IS NOT NULL ORDER BY "${distCol}"`
    );

    // ── Get date range for period label ───────────────────────────
    if (dateCol) {
      const dateRange = await client.query(
        `SELECT MIN("${dateCol}") as min_d, MAX("${dateCol}") as max_d FROM "${salesTable}"`
      );
      if (dateRange.rows[0].min_d) {
        const minD = new Date(dateRange.rows[0].min_d);
        const maxD = new Date(dateRange.rows[0].max_d);
        period = `${minD.toLocaleString('en-IN',{month:'short',year:'numeric'})} — ${maxD.toLocaleString('en-IN',{month:'short',year:'numeric'})}`;
      }
    }

    // ── For each distributor, extract metrics ─────────────────────
    for (const row of distList.rows) {
      const distName = row[distCol];
      if (!distName) continue;

      const d = {
        total_revenue: 0,
        total_orders: 0,
        total_qty: 0,
        total_onboarded: 0,
        total_churned: 0,
        kyc_done: 0,
        monthly_series: [],
        onboard_series: [],
        churn_series: [],
        top_styles_overall: [],
        top_styles_men: [],
        top_styles_women: [],
        top_styles_kids: []
      };

      // Total revenue, orders, qty
      if (revenueCol) {
        const totals = await client.query(
          `SELECT
            COALESCE(SUM("${revenueCol}"),0) as total_rev
            ${ordersCol ? `, COUNT(DISTINCT "${ordersCol}") as total_orders` : ', COUNT(*) as total_orders'}
            ${qtyCol ? `, COALESCE(SUM("${qtyCol}"),0) as total_qty` : ''}
           FROM "${salesTable}"
           WHERE "${distCol}" = $1`,
          [distName]
        );
        d.total_revenue = parseFloat(totals.rows[0].total_rev) || 0;
        d.total_orders  = parseInt(totals.rows[0].total_orders) || 0;
        d.total_qty     = parseInt(totals.rows[0].total_qty) || 0;
      }

      // Monthly series
      if (dateCol && revenueCol) {
        const monthly = await client.query(
          `SELECT
            TO_CHAR(DATE_TRUNC('month', "${dateCol}"::date), 'Mon''YY') as month,
            DATE_TRUNC('month', "${dateCol}"::date) as month_dt,
            COALESCE(SUM("${revenueCol}"),0) as revenue
            ${ordersCol ? `, COUNT(DISTINCT "${ordersCol}") as orders` : ', COUNT(*) as orders'}
            ${qtyCol ? `, COALESCE(SUM("${qtyCol}"),0) as qty` : ''}
           FROM "${salesTable}"
           WHERE "${distCol}" = $1 AND "${dateCol}" IS NOT NULL
           GROUP BY DATE_TRUNC('month',"${dateCol}"::date)
           ORDER BY month_dt`,
          [distName]
        );

        // Calculate MoM %
        d.monthly_series = monthly.rows.map((r, i) => {
          const prev = i > 0 ? parseFloat(monthly.rows[i-1].revenue) : null;
          const curr = parseFloat(r.revenue);
          const mom = (prev && prev > 0) ? parseFloat(((curr - prev) / prev * 100).toFixed(1)) : null;
          return {
            month: r.month,
            revenue: curr,
            orders: parseInt(r.orders) || 0,
            qty: parseInt(r.qty) || 0,
            mom
          };
        });
      }

      // Top styles overall
      if (styleCol && revenueCol) {
        const styles = await client.query(
          `SELECT "${styleCol}" as style, COALESCE(SUM("${revenueCol}"),0) as revenue
           FROM "${salesTable}"
           WHERE "${distCol}" = $1 AND "${styleCol}" IS NOT NULL
           GROUP BY "${styleCol}"
           ORDER BY revenue DESC LIMIT 10`,
          [distName]
        );
        d.top_styles_overall = styles.rows;

        // Top styles by category if category column exists
        if (categoryCol) {
          for (const [key, patterns] of [
            ['top_styles_men',   ['men','male','gents','m']],
            ['top_styles_women', ['women','female','ladies','w','f']],
            ['top_styles_kids',  ['kids','child','children','boy','girl','k']]
          ]) {
            const catStyles = await client.query(
              `SELECT "${styleCol}" as style, COALESCE(SUM("${revenueCol}"),0) as revenue
               FROM "${salesTable}"
               WHERE "${distCol}" = $1
                 AND "${styleCol}" IS NOT NULL
                 AND LOWER("${categoryCol}") = ANY($2::text[])
               GROUP BY "${styleCol}"
               ORDER BY revenue DESC LIMIT 5`,
              [distName, patterns]
            );
            d[key] = catStyles.rows;
          }
        }
      }

      // Onboarding & churn (check separate tables or columns)
      const onbTable = tables.find(t => t.toLowerCase().includes('onboard') || t.toLowerCase().includes('retailer'));
      if (onbTable && onbTable !== salesTable) {
        const onbCols = await getColumns(client, onbTable);
        const oDistCol = findCol(onbCols, ['distributor','dist_name','dealer','partner']);
        const oDateCol = findCol(onbCols, ['date','created_at','onboard_date','month']);
        const oChurnCol = findCol(onbCols, ['churn','churned','status']);

        if (oDistCol && oDateCol) {
          const onbData = await client.query(
            `SELECT TO_CHAR(DATE_TRUNC('month',"${oDateCol}"::date),'Mon''YY') as month,
                    COUNT(*) as count
             FROM "${onbTable}"
             WHERE "${oDistCol}" = $1
             GROUP BY DATE_TRUNC('month',"${oDateCol}"::date)
             ORDER BY DATE_TRUNC('month',"${oDateCol}"::date)`,
            [distName]
          );
          d.total_onboarded = onbData.rows.reduce((s,r)=>s+parseInt(r.count),0);
          d.onboard_series  = onbData.rows;
        }
      }

      // KYC from separate table or column
      const kycTable = tables.find(t => t.toLowerCase().includes('kyc'));
      if (kycTable) {
        const kycCols = await getColumns(client, kycTable);
        const kDistCol = findCol(kycCols, ['distributor','dist_name','dealer']);
        if (kDistCol) {
          const kycData = await client.query(
            `SELECT COUNT(*) as cnt FROM "${kycTable}" WHERE "${kDistCol}" = $1`,
            [distName]
          );
          d.kyc_done = parseInt(kycData.rows[0].cnt) || 0;
        }
      }

      distributors[distName] = d;
    }

    res.json({ distributors, period, lastUpdated: new Date() });

  } catch (err) {
    console.error('Metrics error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// ── TABLES INFO (for debugging) ─────────────────────────────────────
app.get('/tables', async (req, res) => {
  const client = await pool.connect();
  try {
    const tables = await getTables(client);
    const info = {};
    for (const t of tables) {
      const cols = await getColumns(client, t);
      const count = await client.query(`SELECT COUNT(*) FROM "${t}"`);
      info[t] = { columns: cols.map(c=>c.column_name), rows: parseInt(count.rows[0].count) };
    }
    res.json(info);
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.listen(process.env.PORT || 3000, () => {
  console.log(`Server running on port ${process.env.PORT || 3000}`);
});