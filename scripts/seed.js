require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const database = require('../config/database');
const seedCompanies = require('./seedCompanies');
const seedAdmin = require('./seedAdmin');
const seedHierarchy = require('./seedHierarchy');
const seedDeviceDataMapping = require('./seedDeviceDataMapping');
const seedAlarms = require('./seedAlarms');
const seedWidgets = require('./seedWidgets');

const runAllSeeders = async () => {
  let exitCode = 0;
  try {
    console.log('🚀 Starting database initialization and seeding...\n');

    // Connect to database
    await database.connect();

    // Initialize schema (create tables/indexes)
    console.log('🧱 Initializing database schema...');
    await database.initializeSchema();

    // Run all seeders in order
    console.log('📋 Seeding companies...');
    await seedCompanies();

    console.log('👤 Creating admin user...');
    await seedAdmin();

    console.log('🏗️ Seeding hierarchy data...');
    await seedHierarchy();

    console.log('🔗 Seeding device data mappings...');
    await seedDeviceDataMapping();

    console.log('🚨 Seeding alarms data...');
    await seedAlarms();

    console.log('📊 Seeding widgets and dashboard...');
    await seedWidgets();

    console.log('\n✅ All database seeding completed successfully!');
    console.log('\n📋 Summary:');
    console.log('  • Companies and domains configured');
    console.log('  • Admin user created (admin@saherflow.com / Admin123)');
    console.log('  • Test users created for each company');
    console.log('  • Complete hierarchy structure with devices');
    console.log('  • 24 hours of realistic device data');
    console.log('  • Comprehensive alarm system');
    console.log('  • Dynamic widget system configured');
    console.log('\n🚀 You can now start the server with: npm run dev');

  } catch (error) {
    console.error('❌ Error during database seeding:', error);
    exitCode = 1;
  } finally {
    try {
      await database.disconnect();
    } catch (e) {
      console.error('Error while disconnecting:', e);
    }
    process.exit(exitCode);
  }
};

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Seeding interrupted');
  await database.disconnect();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n🛑 Seeding terminated');
  await database.disconnect();
  process.exit(0);
});

runAllSeeders();
  