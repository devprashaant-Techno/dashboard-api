require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.static('public'));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.get('/metrics', async (req, res) => {
  const client = await pool.connect();
  try {
    const metrics  = await client.query('SELECT * FROM dist_metrics ORDER BY total_orders DESC');
    const monthly  = await client.query('SELECT * FROM dist_monthly ORDER BY month');
    const products = await client.query('SELECT * FROM dist_top_products ORDER BY total_qty DESC');

    res.json({
      metrics:      metrics.rows,
      monthly:      monthly.rows,
      top_products: products.rows,
      lastUpdated:  new Date()
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.listen(process.env.PORT || 3000, () => {
  console.log(`Server running on port ${process.env.PORT || 3000}`);
});