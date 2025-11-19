'use strict';

const express = require('express');
const dotenv = require('dotenv');
const database = require('./config/database');
const seedCompanies = require('./scripts/seedCompanies');
const seedAdmin = require('./scripts/seedAdmin');
const cors = require('cors');

// Load environment variables
dotenv.config();

// Validate critical environment variables
const validateEnv = () => {
  if (!process.env.JWT_SECRET) {
    console.error('\n❌ Server startup failed: JWT_SECRET is not configured\n');
    console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.error('Configuration Error: JWT_SECRET environment variable is required');
    console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
    console.error('📋 To fix this error:');
    console.error('1. Open your .env file');
    console.error('2. Add the following line:\n');
    console.error('   JWT_SECRET=your-super-secret-jwt-key-change-this-in-production\n');
    console.error('⚠️  Security Warning: Use a strong, random secret in production!');
    console.error('   Generate one using:');
    console.error('   node -e "console.log(require(\'crypto\').randomBytes(64).toString(\'hex\'))"\n');
    process.exit(1);
  }

  if (process.env.JWT_SECRET.length < 32) {
    console.warn('\n⚠️  Warning: JWT_SECRET is too short (less than 32 characters)');
    console.warn('   For production, use a stronger secret (at least 64 characters)\n');
  }

  console.log('✓ JWT_SECRET configured');
};

validateEnv();

const app = express();

// --- CONFIG ---
const PORT = parseInt(process.env.PORT, 10) || 5000;
const rawClientUrls = process.env.CLIENT_URL || '';
const allowedOrigins = rawClientUrls
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

// include common dev origins as fallback if none provided
// if (allowedOrigins.length === 0) {
//   allowedOrigins.push('http://localhost:5173', 'http://localhost:3001', 'http://localhost:3000');
// }

// --- CORS - manual middleware that reliably sets headers for allowed origins ---

app.use(cors());
 app.use((req, res, next) => {
//   const origin = req.headers.origin;

//   // If no origin (server-to-server or curl) allow it by default
  // if (!origin) {
          res.setHeader('Access-Control-Allow-Origin', '*'); // allow any origin
      res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');
if (req.method === 'OPTIONS') {
       return res.sendStatus(200);
     }
     return next();
   //}

  // // If origin is in allowed list, reflect it and set other CORS headers
  // if (allowedOrigins.includes(origin)) {
  //   res.setHeader('Access-Control-Allow-Origin', origin);
  //   res.setHeader('Access-Control-Allow-Credentials', 'true');
  //   res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
  //   res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');

  //   // Preflight short-circuit
  //   if (req.method === 'OPTIONS') {
  //     return res.sendStatus(200);
  //   }

  //   return next();
  // }

  // // Origin not allowed — respond with 403 JSON (helps debugging in dev)
  // return res.status(403).json({
  //   success: false,
  //   message: `CORS policy: Origin ${origin} is not allowed. Add it to CLIENT_URL in your .env.`
  // });
});


// app.use(cors());

// app.use(cors({
//   origin: "*"
// }));

// --- Body parsers ---
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// --- Routes ---
const authRoutes = require('./routes/auth');
const userRoutes = require('./routes/user');
const companyRoutes = require('./routes/company');
const hierarchyRoutes = require('./routes/hierarchy');
const hierarchyLevelRoutes = require('./routes/hierarchyLevel');
const chartsRoutes = require('./routes/charts');
const devicesRoutes = require('./routes/devices');
const alarmsRoutes = require('./routes/alarms');
const widgetTypesRoutes = require('./routes/widgets');

app.use('/api/auth', authRoutes);
app.use('/api/user', userRoutes);
app.use('/api/company', companyRoutes);
app.use('/api/hierarchy', hierarchyRoutes);
app.use('/api/hierarchy-level', hierarchyLevelRoutes);
app.use('/api/charts', chartsRoutes);
app.use('/api/devices', devicesRoutes);
app.use('/api/alarms', alarmsRoutes);
app.use('/api/widgets', widgetTypesRoutes);

// Health check
app.get('/api/health', (req, res) => {
  res.json({
    message: 'Saher Flow Solutions API is running!',
    timestamp: new Date().toISOString()
  });
});

// 404 handler (after routes)
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    message: `Route not found: ${req.originalUrl}`
  });
});

// Error handler (last middleware)
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err && err.stack ? err.stack : err);
  res.status(500).json({
    success: false,
    message: 'Something went wrong!',
    error: process.env.NODE_ENV === 'development' ? (err && err.message) : 'Internal server error'
  });
});

// --- Database connection & server start ---
let server;

const startServer = async () => {
  await database.connect();

  // Run seeders (do not block start if they fail)
  //seedCompanies().catch((err) => console.error('seedCompanies error:', err));
  //seedAdmin().catch((err) => console.error('seedAdmin error:', err));
  
  // Seed device types
  // Seed hierarchy data
  //const seedHierarchy = require('./utils/seedHierarchy');
  //seedHierarchy().catch((err) => console.error('seedHierarchy error:', err));

  server = app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/api/health`);
    console.log('Allowed CORS origins:', allowedOrigins);
  });

  // Graceful handling of listen errors (e.g. EADDRINUSE)
  // server.on('error', (err) => {
  //   if (err && err.code === 'EADDRINUSE') {
  //     console.error(`Port ${PORT} is already in use (EADDRINUSE).`);
  //     console.error('Options:');
  //     console.error(`  - Kill the process using the port (Windows: netstat -ano | findstr :${PORT} -> taskkill /PID <pid> /F)`);
  //     console.error(`  - Use a different PORT by setting PORT env variable (e.g. PORT=${PORT + 1} npm run dev)`);
  //     console.error(`  - Use npx kill-port ${PORT}`);
  //     process.exit(1);
  //   } else {
  //     console.error('Server error:', err);
  //     process.exit(1);
  //   }
  // });
};

// Graceful shutdown
const gracefulShutdown = async (signal) => {
  console.log(`\nReceived ${signal}. Shutting down gracefully...`);
  try {
    if (server) {
      server.close(() => {
        console.log('HTTP server closed.');
      });
    }
    await database.disconnect();
    process.exit(0);
  } catch (err) {
    console.error('Error during shutdown:', err);
    process.exit(1);
  }
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// Start
startServer().catch((err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});

// Export app for tests
module.exports = app;
