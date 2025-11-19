const Company = require('../models/Company');

// Initial companies data - updated to use Arabco instead of Aramco
const initialCompanies = [
  {
    name: 'Arabco',
    domain_name: 'arabco.com'
  },
  {
    name: 'Saher Flow',
    domain_name: 'saherflow.com'
  }
];

const seedCompanies = async () => {
  try {
    // Check if companies already exist
    const existingCompanies = await Company.findAll();
    
    if (existingCompanies.length > 0) {
      console.log('Companies already exist in database. Skipping seed.');
      return;
    }

    // Insert initial companies
    for (const companyData of initialCompanies) {
      await Company.create(companyData);
    }
    
    console.log('✅ Initial companies seeded successfully');
    
    // Log the seeded companies
    const companies = await Company.findAll();
    console.log('\n📋 Approved companies and domains:');
    companies.forEach(company => {
      console.log(`  • ${company.name}: ${company.domain_name}`);
    });
    console.log('');
    
  } catch (error) {
    console.error('❌ Error seeding companies:', error);
    throw error;
  }
};

module.exports = seedCompanies;