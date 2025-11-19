const database = require('../config/database');

class Hierarchy {
  constructor(data = {}) {
    this.id = data.id;
    this.company_id = data.company_id;
    this.name = data.name;
    this.level_id = data.level_id;
    this.parent_id = data.parent_id;
    this.can_attach_device = data.can_attach_device !== undefined ? data.can_attach_device : true;
    this.created_at = data.created_at;
    this.updated_at = data.updated_at;
    this.level_name = data.level_name;
    this.level_order = data.level_order;
    this.company_name = data.company_name;
    this.parent_name = data.parent_name;
  }

  static async findById(id) {
    const query = `
      SELECT h.*, hl.name as level_name, hl.level_order, c.name as company_name, 
             ph.name as parent_name
      FROM hierarchy h
      JOIN hierarchy_level hl ON h.level_id = hl.id
      JOIN company c ON h.company_id = c.id
      LEFT JOIN hierarchy ph ON h.parent_id = ph.id
      WHERE h.id = $1
    `;
    const result = await database.query(query, [id]);
    return result.rows[0] ? new Hierarchy(result.rows[0]) : null;
  }

  static async findByCompany(company_id) {
    const query = `
      SELECT h.*, hl.name as level_name, hl.level_order, c.name as company_name,
             ph.name as parent_name
      FROM hierarchy h
      JOIN hierarchy_level hl ON h.level_id = hl.id
      JOIN company c ON h.company_id = c.id
      LEFT JOIN hierarchy ph ON h.parent_id = ph.id
      WHERE h.company_id = $1
      ORDER BY hl.level_order, h.name
    `;
    const result = await database.query(query, [company_id]);
    return result.rows.map(row => new Hierarchy(row));
  }

  static async getHierarchyTree(company_id = null) {
    let query = `
      SELECT 
        h.id, h.name, h.parent_id, h.company_id, h.can_attach_device, h.created_at, h.level_id,
        hl.name AS level_name, hl.level_order, hl.icon AS level_icon,
        c.name AS company_name,
        d.id as device_id, d.serial_number, d.metadata, d.created_at as device_created_at,
        dt.type_name as device_type, dt.logo as device_logo,
        dl.data as latest_data, dl.updated_at as latest_data_time
      FROM hierarchy h
      JOIN hierarchy_level hl ON h.level_id = hl.id
      JOIN company c ON h.company_id = c.id
      LEFT JOIN device d ON h.id = d.hierarchy_id
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN device_latest dl ON d.serial_number= dl.serial_number
    `;
    
    const params = [];
    if (company_id) {
      query += ' WHERE h.company_id = $1';
      params.push(company_id);
    }
    
    query += ' ORDER BY c.name, hl.level_order, h.name';
    
    const result = await database.query(query, params);
    
    // Organize hierarchical data by company
    const companies = {};
    const nodeMap = {};

    result.rows.forEach(row => {
      if (!companies[row.company_name]) {
        companies[row.company_name] = {
          id: row.company_id,
          name: row.company_name,
          hierarchy: [],
          statistics: {
            totalNodes: 0,
            regions: 0,
            areas: 0,
            fields: 0,
            wells: 0,
            devices: 0
          }
        };
      }

      if (!nodeMap[row.id]) {
        nodeMap[row.id] = {
          id: row.id,
          name: row.name,
          level_id: row.level_id,
          level: row.level_name,
          level_order: row.level_order,
          level_icon: row.level_icon,
          parent_id: row.parent_id,
          can_attach_device: row.can_attach_device,
          created_at: row.created_at,
          children: [],
          devices: [],
          company_id: row.company_id
        };
        
        // Update statistics
        companies[row.company_name].statistics.totalNodes++;
        if (row.level_name === 'Region') companies[row.company_name].statistics.regions++;
        else if (row.level_name === 'Area') companies[row.company_name].statistics.areas++;
        else if (row.level_name === 'Field') companies[row.company_name].statistics.fields++;
        else if (row.level_name === 'Well') companies[row.company_name].statistics.wells++;
      }

      // Add device info if exists
      if (row.device_id) {
        const deviceMetadata = row.metadata || {};
        const existingDevice = nodeMap[row.id].devices.find(d => d.id === row.device_id);
        
        if (!existingDevice) {
          companies[row.company_name].statistics.devices++;
          nodeMap[row.id].devices.push({
            id: row.device_id,
            serial_number: row.serial_number,
            type: row.device_type,
            logo: row.device_logo,
            metadata: deviceMetadata,
            status: deviceMetadata.status || 'active',
            created_at: row.device_created_at,
            latest_data: row.latest_data,
            latest_data_time: row.latest_data_time
          });
        }
      }
    });

    // Build hierarchy tree
    const addedNodes = new Set();
    result.rows.forEach(row => {
      const node = nodeMap[row.id];
      if (row.parent_id && nodeMap[row.parent_id] && !addedNodes.has(row.id)) {
        nodeMap[row.parent_id].children.push(node);
        addedNodes.add(row.id);
      } else if (!row.parent_id && !addedNodes.has(row.id)) {
        companies[row.company_name].hierarchy.push(node);
        addedNodes.add(row.id);
      }
    });

    return companies;
  }

  toJSON() {
    return {
      id: this.id,
      company_id: this.company_id,
      name: this.name,
      level_id: this.level_id,
      parent_id: this.parent_id,
      can_attach_device: this.can_attach_device,
      created_at: this.created_at,
      updated_at: this.updated_at,
      level_name: this.level_name,
      level_order: this.level_order,
      company_name: this.company_name,
      parent_name: this.parent_name
    };
  }
}

module.exports = Hierarchy;