const database = require('../config/database');

class HierarchyLevel {
  constructor(data = {}) {
    this.id = data.id;
    this.level_order = data.level_order;
    this.name = data.name;
    this.icon = data.icon;
    this.created_at = data.created_at;
  }

  static async findById(id) {
    const query = 'SELECT * FROM hierarchy_level WHERE id = $1';
    const result = await database.query(query, [id]);
    return result.rows[0] ? new HierarchyLevel(result.rows[0]) : null;
  }

  static async findAll() {
    const query = 'SELECT * FROM hierarchy_level ORDER BY level_order ASC';
    const result = await database.query(query);
    return result.rows.map(row => new HierarchyLevel(row));
  }

  toJSON() {
    return {
      id: this.id,
      level_order: this.level_order,
      name: this.name,
      icon: this.icon,
      created_at: this.created_at
    };
  }
}

module.exports = HierarchyLevel;