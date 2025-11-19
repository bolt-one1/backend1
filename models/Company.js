const database = require('../config/database');
const validator = require('validator');

class Company {
  constructor(data = {}) {
    this.id = data.id;
    this.name = data.name;
    this.domain_name = data.domain_name;
    this.created_at = data.created_at;
  }

  static async create(companyData) {
    const { name, domain_name } = companyData;

    // Validate domain
    if (domain_name && !validator.isFQDN(domain_name, { require_tld: true, allow_underscores: false, allow_trailing_dot: false })) {
      throw new Error('Invalid domain format');
    }

    const query = `
      INSERT INTO company (name, domain_name)
      VALUES ($1, $2)
      RETURNING *
    `;
    
    const result = await database.query(query, [name, domain_name?.toLowerCase()]);
    return new Company(result.rows[0]);
  }

  static async findById(id) {
    const query = 'SELECT * FROM company WHERE id = $1';
    const result = await database.query(query, [id]);
    return result.rows[0] ? new Company(result.rows[0]) : null;
  }

  static async findByDomain(domain) {
    const normalizedDomain = domain.toLowerCase();
    const query = 'SELECT * FROM company WHERE domain_name = $1';
    const result = await database.query(query, [normalizedDomain]);
    return result.rows[0] ? new Company(result.rows[0]) : null;
  }

  static async findAll() {
    const query = 'SELECT * FROM company ORDER BY name ASC';
    const result = await database.query(query);
    return result.rows.map(row => new Company(row));
  }

  static async update(id, updateData) {
    const { name, domain_name } = updateData;
    
    // Validate domain if provided
    if (domain_name && !validator.isFQDN(domain_name, { require_tld: true, allow_underscores: false, allow_trailing_dot: false })) {
      throw new Error('Invalid domain format');
    }

    const query = `
      UPDATE company 
      SET name = COALESCE($2, name),
          domain_name = COALESCE($3, domain_name)
      WHERE id = $1
      RETURNING *
    `;
    
    const result = await database.query(query, [id, name, domain_name?.toLowerCase()]);
    return result.rows[0] ? new Company(result.rows[0]) : null;
  }

  static async delete(id) {
    const query = 'DELETE FROM company WHERE id = $1 RETURNING *';
    const result = await database.query(query, [id]);
    return result.rows[0] ? new Company(result.rows[0]) : null;
  }

  static async checkDomainExists(domain, excludeId = null) {
    const normalizedDomain = domain.toLowerCase();
    let query = 'SELECT id FROM company WHERE domain_name = $1';
    const params = [normalizedDomain];
    
    if (excludeId) {
      query += ' AND id != $2';
      params.push(excludeId);
    }
    
    const result = await database.query(query, params);
    return result.rows.length > 0;
  }

  static async checkNameExists(name, excludeId = null) {
    let query = 'SELECT id FROM company WHERE LOWER(name) = LOWER($1)';
    const params = [name];
    
    if (excludeId) {
      query += ' AND id != $2';
      params.push(excludeId);
    }
    
    const result = await database.query(query, params);
    return result.rows.length > 0;
  }

  hasDomain(domain) {
    const normalizedDomain = domain.toLowerCase();
    return this.domain_name === normalizedDomain || normalizedDomain.endsWith('.' + this.domain_name);
  }

  toJSON() {
    return {
      id: this.id,
      name: this.name,
      domains: this.domain_name ? [this.domain_name] : [], // For compatibility with frontend
      domain_name: this.domain_name,
      created_at: this.created_at,
      isActive: true // For compatibility with frontend
    };
  }
}

module.exports = Company;