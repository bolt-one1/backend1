const database = require('../config/database');

class AlarmStatus {
  constructor(data = {}) {
    this.id = data.id;
    this.name = data.name;
    this.description = data.description;
    this.created_at = data.created_at;
  }

  static async findById(id) {
    const query = 'SELECT * FROM alarm_status_type WHERE id = $1';
    const result = await database.query(query, [id]);
    return result.rows[0] ? new AlarmStatus(result.rows[0]) : null;
  }

  static async findAll() {
    const query = 'SELECT * FROM alarm_status_type ORDER BY name ASC';
    const result = await database.query(query);
    return result.rows.map(row => new AlarmStatus(row));
  }

  static async create(statusData) {
    const { name, description } = statusData;
    
    const query = `
      INSERT INTO alarm_status_type (name, description)
      VALUES ($1, $2)
      RETURNING *
    `;
    
    const result = await database.query(query, [name, description]);
    return new AlarmStatus(result.rows[0]);
  }

  toJSON() {
    return {
      id: this.id,
      name: this.name,
      description: this.description,
      created_at: this.created_at
    };
  }
}

module.exports = AlarmStatus;