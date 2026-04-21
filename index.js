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
  ssl: { rejectUnauthorized: false }
});

app.get('/metrics', async (req, res) => {
  try {
    const client = await pool.connect();

    const [total, latest, summary] = await Promise.all([
      client.query('SELECT COUNT(*) AS total FROM students'),
      client.query('SELECT * FROM students ORDER BY created_at DESC LIMIT 5'),
      client.query(`
        SELECT DATE(created_at) AS date, COUNT(*) AS count
        FROM students
        GROUP BY DATE(created_at)
        ORDER BY date DESC
        LIMIT 7
      `)
    ]);

    client.release();

    res.json({
      totalRecords: total.rows[0].total,
      latestEntries: latest.rows,
      dailySummary: summary.rows,
      lastUpdated: new Date().toISOString()
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(process.env.PORT || 3000, () => {
  console.log(`Server running on port ${process.env.PORT || 3000}`);
});