const database = require('../config/database');

const seedAlarmTables = async () => {
  try {
    // Create alarm_types table
    await database.query(`
      CREATE TABLE IF NOT EXISTS alarm_types (
        id BIGSERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        severity TEXT NOT NULL CHECK (severity IN ('Critical', 'Major', 'Minor', 'Warning')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);

    // Create alarm_status_type table
    await database.query(`
      CREATE TABLE IF NOT EXISTS alarm_status_type (
        id BIGSERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);

    // Create device_alarms table
    await database.query(`
      CREATE TABLE IF NOT EXISTS device_alarms (
        id BIGSERIAL PRIMARY KEY,
        device_serial TEXT NOT NULL,
        alarm_type_id BIGINT NOT NULL REFERENCES alarm_types(id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        status_id BIGINT NOT NULL REFERENCES alarm_status_type(id) ON DELETE CASCADE,
        acknowledged_by BIGINT REFERENCES "user"(id),
        acknowledged_at TIMESTAMPTZ,
        resolved_by BIGINT REFERENCES "user"(id),
        resolved_at TIMESTAMPTZ,
        message TEXT,
        metadata JSONB DEFAULT '{}'::jsonb
      )
    `);

    // Create indexes
    await database.query(`
      CREATE INDEX IF NOT EXISTS idx_device_alarms_device_serial ON device_alarms(device_serial);
      CREATE INDEX IF NOT EXISTS idx_device_alarms_alarm_type_id ON device_alarms(alarm_type_id);
      CREATE INDEX IF NOT EXISTS idx_device_alarms_status_id ON device_alarms(status_id);
      CREATE INDEX IF NOT EXISTS idx_device_alarms_created_at ON device_alarms(created_at DESC);
      CREATE INDEX IF NOT EXISTS gin_device_alarms_metadata ON device_alarms USING gin(metadata);
    `);

    console.log('✅ Alarm tables created successfully');
  } catch (error) {
    console.error('❌ Error creating alarm tables:', error);
    throw error;
  }
};

const seedAlarmTypes = async () => {
  try {
    // Clear existing alarm types
    await database.query('TRUNCATE TABLE alarm_types RESTART IDENTITY CASCADE');

    const alarmTypes = [
      { id: 1, name: 'Pipeline Monitor', severity: 'Minor' },
      { id: 2, name: 'Flow Low', severity: 'Major' },
      { id: 3, name: 'Flow High', severity: 'Warning' },
      { id: 4, name: 'Flow Analyzer', severity: 'Warning' },
      { id: 5, name: 'Separator Unit', severity: 'Critical' },
      { id: 6, name: 'Pressure High', severity: 'Critical' },
      { id: 7, name: 'Pressure Low', severity: 'Major' },
      { id: 8, name: 'Temperature High', severity: 'Major' },
      { id: 9, name: 'Temperature Low', severity: 'Minor' },
      { id: 10, name: 'Vibration High', severity: 'Warning' },
      { id: 11, name: 'Communication Lost', severity: 'Critical' },
      { id: 12, name: 'Device Offline', severity: 'Major' },
      { id: 13, name: 'Calibration Required', severity: 'Minor' },
      { id: 14, name: 'Maintenance Due', severity: 'Warning' },
      { id: 15, name: 'Power Supply Issue', severity: 'Critical' }
    ];

    for (const alarmType of alarmTypes) {
      await database.query(`
        INSERT INTO alarm_types (id, name, severity) 
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO NOTHING
      `, [alarmType.id, alarmType.name, alarmType.severity]);
    }

    console.log('✅ Alarm types seeded successfully');
  } catch (error) {
    console.error('❌ Error seeding alarm types:', error);
    throw error;
  }
};

const seedAlarmStatuses = async () => {
  try {
    // Clear existing alarm statuses
    await database.query('TRUNCATE TABLE alarm_status_type RESTART IDENTITY CASCADE');

    const alarmStatuses = [
      { id: 1, name: 'Active', description: 'Alarm is currently active and requires attention' },
      { id: 2, name: 'Acknowledged', description: 'Alarm has been acknowledged by an operator' },
      { id: 3, name: 'Resolved', description: 'Alarm has been resolved and is no longer active' },
      { id: 4, name: 'Unacked', description: 'Alarm is active but not yet acknowledged' }
    ];

    for (const status of alarmStatuses) {
      await database.query(`
        INSERT INTO alarm_status_type (id, name, description) 
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO NOTHING
      `, [status.id, status.name, status.description]);
    }

    console.log('✅ Alarm statuses seeded successfully');
  } catch (error) {
    console.error('❌ Error seeding alarm statuses:', error);
    throw error;
  }
};

const seedDeviceAlarms = async () => {
  try {
    // Clear existing device alarms
    // Check if alarms already exist
    const existingAlarms = await database.query('SELECT COUNT(*) as count FROM device_alarms');
    if (parseInt(existingAlarms.rows[0].count) > 0) {
      console.log('Device alarms already exist. Skipping seed.');
      return;
    }

    // Get all devices to create realistic alarms
    const devicesResult = await database.query(`
      SELECT d.serial_number, d.company_id, dt.type_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      ORDER BY d.id
    `);

    const devices = devicesResult.rows;
    
    if (devices.length === 0) {
      console.log('No devices found. Cannot create alarms.');
      return;
    }
    
    const alarmData = [];

    // Generate realistic alarms for devices
    const now = new Date();

    for (const device of devices) {
      // Generate 2-5 alarms per device
      const numAlarms = Math.floor(Math.random() * 4) + 2;
      
      for (let i = 0; i < numAlarms; i++) {
        // Random alarm type based on device type
        let alarmTypeId;
        if (device.type_name === 'MPFM') {
          alarmTypeId = [1, 2, 3, 4, 5][Math.floor(Math.random() * 5)];
        } else if (device.type_name === 'Pressure Sensor') {
          alarmTypeId = [6, 7, 11, 12][Math.floor(Math.random() * 4)];
        } else if (device.type_name === 'Temperature Sensor') {
          alarmTypeId = [8, 9, 11, 12][Math.floor(Math.random() * 4)];
        } else if (device.type_name === 'Flow Meter') {
          alarmTypeId = [2, 3, 11, 12][Math.floor(Math.random() * 4)];
        } else {
          alarmTypeId = [10, 11, 12, 13, 14][Math.floor(Math.random() * 5)];
        }

        // Random status (weighted towards active/unacked)
        const statusOptions = [1, 1, 1, 2, 3]; // More active alarms
        const statusId = statusOptions[Math.floor(Math.random() * statusOptions.length)];

        // Random time in the last 30 days
        const createdAt = new Date(now.getTime() - (Math.random() * 30 * 24 * 60 * 60 * 1000));
        
        // Generate message based on alarm type
        const messages = {
          1: 'Pipeline monitoring system detected anomaly',
          2: 'Flow rate below minimum threshold',
          3: 'Flow rate exceeds maximum threshold',
          4: 'Flow analyzer requires calibration',
          5: 'Separator unit malfunction detected',
          6: 'Pressure reading above critical limit',
          7: 'Pressure reading below operational minimum',
          8: 'Temperature exceeds safe operating range',
          9: 'Temperature below operational minimum',
          10: 'Vibration levels exceed normal range',
          11: 'Communication with device lost',
          12: 'Device is offline or not responding',
          13: 'Device calibration is overdue',
          14: 'Scheduled maintenance is due',
          15: 'Power supply voltage out of range'
        };

        const metadata = {
          device_type: device.type_name,
          auto_generated: true,
          source: 'system_monitor'
        };

        alarmData.push({
          device_serial: device.serial_number,
          alarm_type_id: alarmTypeId,
          message: messages[alarmTypeId] || 'System alarm triggered',
          metadata: JSON.stringify(metadata),
          status_id: statusId,
          created_at: createdAt,
          updated_at: createdAt
        });
      }
    }

    // Insert alarms in batches
    const batchSize = 50;
    for (let i = 0; i < alarmData.length; i += batchSize) {
      const batch = alarmData.slice(i, i + batchSize);
      const values = [];
      const placeholders = [];
      
      batch.forEach((alarm, index) => {
        const baseIndex = index * 7;
        placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7})`);
        values.push(
          alarm.device_serial,
          alarm.alarm_type_id,
          alarm.message,
          alarm.metadata,
          alarm.status_id,
          alarm.created_at,
          alarm.updated_at
        );
      });
      
      if (values.length > 0) {
        await database.query(`
          INSERT INTO device_alarms (device_serial, alarm_type_id, message, metadata, status_id, created_at, updated_at) 
          VALUES ${placeholders.join(', ')}
        `, values);
      }
    }

    console.log('✅ Device alarms seeded successfully');
    console.log(`📊 Created ${alarmData.length} device alarms`);

    // Show summary statistics
    const statsResult = await database.query(`
      SELECT 
        ast.name as status,
        COUNT(*) as count
      FROM device_alarms da
      JOIN alarm_status_type ast ON da.status_id = ast.id
      GROUP BY ast.name
      ORDER BY count DESC
    `);

    console.log('\n📋 Alarm Status Summary:');
    statsResult.rows.forEach(row => {
      console.log(`  ${row.status}: ${row.count} alarms`);
    });

    const severityStats = await database.query(`
      SELECT 
        at.severity,
        COUNT(*) as count
      FROM device_alarms da
      JOIN alarm_types at ON da.alarm_type_id = at.id
      GROUP BY at.severity
      ORDER BY 
        CASE at.severity 
          WHEN 'Critical' THEN 1 
          WHEN 'Major' THEN 2 
          WHEN 'Minor' THEN 3 
          WHEN 'Warning' THEN 4 
        END
    `);

    console.log('\n🚨 Alarm Severity Summary:');
    severityStats.rows.forEach(row => {
      console.log(`  ${row.severity}: ${row.count} alarms`);
    });
    console.log('');

  } catch (error) {
    console.error('❌ Error seeding device alarms:', error);
    throw error;
  }
};

const seedAlarms = async () => {
  await seedAlarmTables();
  await seedAlarmTables();
  await seedAlarmTypes();
  await seedAlarmStatuses();
  await seedDeviceAlarms();
};

module.exports = seedAlarms;