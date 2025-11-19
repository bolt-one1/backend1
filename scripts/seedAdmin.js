const User = require('../models/User');
const Company = require('../models/Company');

const createAdminUser = async () => {
  try {
    // Check if admin already exists
    const existingAdmin = await User.findByEmail('admin@saherflow.com');
    
    if (existingAdmin) {
      console.log('Admin user already exists');
      return;
    }

    // Find or create Saher Flow company
    let company = await Company.findByDomain('saherflow.com');
    if (!company) {
      company = await Company.create({
        name: 'Saher Flow',
        domain_name: 'saherflow.com'
      });
    }

    // Create admin user
    const adminUser = await User.create({
      company_id: company.id,
      name: 'Admin User',
      email: 'admin@saherflow.com',
      password: 'Admin123',
      role: 'admin'
    });

    // Mark as verified
    const query = 'UPDATE "user" SET is_email_verified = true, email_validated = true WHERE id = $1';
    await require('../config/database').query(query, [adminUser.id]);

    console.log('✅ Admin user created successfully');
    console.log('📧 Email: admin@saherflow.com');
    console.log('🔑 Password: Admin123');
    console.log('');

    // Create test users for each company
    const testUsers = [
      {
        company: 'Arabco',
        domain: 'arabco.com',
        email: 'john@arabco.com',
        name: 'John Doe'
      },
      {
        company: 'Saher Flow',
        domain: 'saherflow.com',
        email: 'jane@saherflow.com',
        name: 'Jane Smith'
      }
    ];

    for (const testUser of testUsers) {
      const existingUser = await User.findByEmail(testUser.email);
      if (!existingUser) {
        const userCompany = await Company.findByDomain(testUser.domain);
        if (userCompany) {
          const user = await User.create({
            company_id: userCompany.id,
            name: testUser.name,
            email: testUser.email,
            password: 'Password123',
            role: 'user'
          });

          // Mark as verified
          await require('../config/database').query(
            'UPDATE "user" SET is_email_verified = true, email_validated = true WHERE id = $1',
            [user.id]
          );

          console.log(`✅ Test user created: ${testUser.email} (${testUser.company})`);
        }
      }
    }
    
  } catch (error) {
    console.error('❌ Error creating admin user:', error);
  }
};

module.exports = createAdminUser;