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
    const total = await client.query('SELECT COUNT(*) as total FROM students');
    const latest = await client.query('SELECT * FROM students ORDER BY created_at DESC LIMIT 5');
    const daily = await client.query(`
      SELECT DATE(created_at) as date, COUNT(*) as count
      FROM students
      GROUP BY DATE(created_at)
      ORDER BY date DESC LIMIT 7
    `);

    res.json({
      totalRecords: total.rows[0].total,
      latestEntries: latest.rows,
      dailySummary: daily.rows,
      lastUpdated: new Date()
    });

  } catch (err) {
    console.error('Metrics error:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.listen(process.env.PORT || 3000, () => {
  console.log(`Server running on port ${process.env.PORT || 3000}`);
});