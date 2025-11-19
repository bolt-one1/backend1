const database = require('../config/database');

const seedDeviceDataMapping = async () => {
  const client = await database.pool.connect();

  try {
    await client.query('BEGIN');

    const mpfmDeviceType = await client.query(
      `SELECT id FROM device_type WHERE type_name = 'MPFM' LIMIT 1`
    );

    if (mpfmDeviceType.rows.length === 0) {
      throw new Error('MPFM device type not found');
    }

    const deviceTypeId = mpfmDeviceType.rows[0].id;

    await client.query(
      `DELETE FROM device_data_mapping WHERE device_type_id = $1`,
      [deviceTypeId]
    );

    const mappings = [
      { variable_name: 'Gas Flow Rate', variable_tag: 'GFR', unit: 'l/min', data_type: 'numeric', ui_order: 1, expression: null, requires_post_aggregation: false },
      { variable_name: 'Gas Oil Ratio', variable_tag: 'GOR', unit: 'ratio', data_type: 'numeric', ui_order: 2, expression: null, requires_post_aggregation: false },
      { variable_name: 'Gas Volume Fraction', variable_tag: 'GVF', unit: '%', data_type: 'numeric', ui_order: 3, expression: 'GFR / (GFR + OFR + WFR) * 100', requires_post_aggregation: true },
      { variable_name: 'Oil Flow Rate', variable_tag: 'OFR', unit: 'l/min', data_type: 'numeric', ui_order: 4, expression: null, requires_post_aggregation: false },
      { variable_name: 'Water Flow Rate', variable_tag: 'WFR', unit: 'l/min', data_type: 'numeric', ui_order: 5, expression: null, requires_post_aggregation: false },
      { variable_name: 'Water Liquid Ratio', variable_tag: 'WLR', unit: '%', data_type: 'numeric', ui_order: 6, expression: 'WFR / (WFR + OFR) * 100', requires_post_aggregation: true },
      { variable_name: 'Pressure Average', variable_tag: 'PressureAvg', unit: 'bar', data_type: 'numeric', ui_order: 7, expression: null, requires_post_aggregation: false },
      { variable_name: 'Temperature Average', variable_tag: 'TemperatureAvg', unit: '°C', data_type: 'numeric', ui_order: 8, expression: null, requires_post_aggregation: false }
    ];

    for (const mapping of mappings) {
      await client.query(
        `INSERT INTO device_data_mapping (device_type_id, variable_name, variable_tag, unit, data_type, ui_order, expression, requires_post_aggregation)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [deviceTypeId, mapping.variable_name, mapping.variable_tag, mapping.unit, mapping.data_type, mapping.ui_order, mapping.expression, mapping.requires_post_aggregation]
      );
    }

    await client.query('COMMIT');
    console.log('✅ Device data mappings seeded successfully for MPFM');
    console.log(`   • Created ${mappings.length} property mappings`);
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('❌ Error seeding device data mappings:', error);
    throw error;
  } finally {
    client.release();
  }
};

module.exports = seedDeviceDataMapping;

if (require.main === module) {
  (async () => {
    try {
      await database.connect();
      await seedDeviceDataMapping();
      await database.disconnect();
      process.exit(0);
    } catch (error) {
      console.error(error);
      process.exit(1);
    }
  })();
}
