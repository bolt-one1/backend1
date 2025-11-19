const database = require('../config/database');

class AlarmType {
  constructor(data = {}) {
    this.id = data.id;
    this.name = data.name;
    this.severity = data.severity;
    this.created_at = data.created_at;
  }

  static async findById(id) {
    const query = 'SELECT * FROM alarm_types WHERE id = $1';
    const result = await database.query(query, [id]);
    return result.rows[0] ? new AlarmType(result.rows[0]) : null;
  }

  static async findAll() {
    const query = 'SELECT * FROM alarm_types ORDER BY severity DESC, name ASC';
    const result = await database.query(query);
    return result.rows.map(row => new AlarmType(row));
  }

  static async create(alarmTypeData) {
    const { name, severity } = alarmTypeData;
    
    const query = `
      INSERT INTO alarm_types (name, severity)
      VALUES ($1, $2)
      RETURNING *
    `;
    
    const result = await database.query(query, [name, severity]);
    return new AlarmType(result.rows[0]);
  }

  static async update(id, updateData) {
    const { name, severity } = updateData;
    
    const query = `
      UPDATE alarm_types 
      SET name = COALESCE($2, name),
          severity = COALESCE($3, severity)
      WHERE id = $1
      RETURNING *
    `;
    
    const result = await database.query(query, [id, name, severity]);
    return result.rows[0] ? new AlarmType(result.rows[0]) : null;
  }

  static async delete(id) {
    const query = 'DELETE FROM alarm_types WHERE id = $1 RETURNING *';
    const result = await database.query(query, [id]);
    return result.rows[0] ? new AlarmType(result.rows[0]) : null;
  }

  toJSON() {
    return {
      id: this.id,
      name: this.name,
      severity: this.severity,
      created_at: this.created_at
    };
  }
}

module.exports = AlarmType;