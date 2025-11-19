require('dotenv').config(); 
const database = require('../config/database');

// Realistic coordinates for Saudi Arabia oil fields and regions
const locationData = {
  // Arabco Company locations (Eastern Province focus)
  arabco: {
    // Eastern Region - Ghawar Field area
    ghawar: {
      center: { lat: 25.0657, lng: 49.3479 },
      wells: [
        { lat: 25.0657, lng: 49.3479 }, // Well-101
        { lat: 25.0712, lng: 49.3521 }, // Well-102  
        { lat: 25.0598, lng: 49.3435 }  // Well-103
      ]
    },
    // Shaybah Field area
    shaybah: {
      center: { lat: 22.5167, lng: 53.9667 },
      wells: [
        { lat: 22.5167, lng: 53.9667 }, // Well-201
        { lat: 22.5223, lng: 53.9712 }  // Well-202
      ]
    },
    // Berri Field area
    berri: {
      center: { lat: 26.6833, lng: 49.6167 },
      wells: [
        { lat: 26.6833, lng: 49.6167 }, // Well-301
        { lat: 26.6889, lng: 49.6223 }  // Well-302
      ]
    },
    // Red Sea Field area (Western Region)
    redSea: {
      center: { lat: 21.4858, lng: 39.1925 },
      wells: [
        { lat: 21.4858, lng: 39.1925 }, // Well-401
        { lat: 21.4912, lng: 39.1981 }  // Well-402
      ]
    }
  },
  
  // Saher Flow Company locations (Demo areas)
  saherFlow: {
    // KSA Demo Area
    demoAlpha: {
      center: { lat: 24.7136, lng: 46.6753 },
      wells: [
        { lat: 24.7136, lng: 46.6753 }, // Demo Well A1
        { lat: 24.7192, lng: 46.6809 }  // Demo Well A2
      ]
    },
    demoBeta: {
      center: { lat: 24.6892, lng: 46.7021 },
      wells: [
        { lat: 24.6892, lng: 46.7021 }, // Demo Well B1
        { lat: 24.6948, lng: 46.7077 }  // Demo Well B2
      ]
    },
    // UAE Demo Area
    dubai: {
      center: { lat: 25.2048, lng: 55.2708 },
      wells: [
        { lat: 25.2048, lng: 55.2708 }, // Well-D1
        { lat: 25.2104, lng: 55.2764 }  // Well-D2
      ]
    },
    // Egypt Demo Area
    cairo: {
      center: { lat: 30.0444, lng: 31.2357 },
      wells: [
        { lat: 30.0444, lng: 31.2357 }, // Well-E1
        { lat: 30.0500, lng: 31.2413 }  // Well-E2
      ]
    }
  }
};

const updateDeviceLocations = async () => {
  try {
    console.log('🗺️ Updating device locations with realistic coordinates...');

    // Get all devices with their hierarchy information
    const devicesQuery = `
      SELECT 
        d.id,
        d.serial_number,
        d.hierarchy_id,
        h.name as hierarchy_name,
        h.company_id,
        c.name as company_name,
        hl.name as level_name
      FROM device d
      JOIN hierarchy h ON d.hierarchy_id = h.id
      JOIN company c ON h.company_id = c.id
      JOIN hierarchy_level hl ON h.level_id = hl.id
      ORDER BY d.id
    `;

    const result = await database.query(devicesQuery);
    const devices = result.rows;

    console.log(`Found ${devices.length} devices to update`);

    // Update locations for each device
    for (const device of devices) {
      let coordinates = null;
      
      if (device.company_name === 'Arabco') {
        // Arabco devices
        if (device.hierarchy_name.includes('Well-101')) {
          coordinates = locationData.arabco.ghawar.wells[0];
        } else if (device.hierarchy_name.includes('Well-102')) {
          coordinates = locationData.arabco.ghawar.wells[1];
        } else if (device.hierarchy_name.includes('Well-103')) {
          coordinates = locationData.arabco.ghawar.wells[2];
        } else if (device.hierarchy_name.includes('Well-201')) {
          coordinates = locationData.arabco.shaybah.wells[0];
        } else if (device.hierarchy_name.includes('Well-202')) {
          coordinates = locationData.arabco.shaybah.wells[1];
        } else if (device.hierarchy_name.includes('Well-301')) {
          coordinates = locationData.arabco.berri.wells[0];
        } else if (device.hierarchy_name.includes('Well-302')) {
          coordinates = locationData.arabco.berri.wells[1];
        } else if (device.hierarchy_name.includes('Well-401')) {
          coordinates = locationData.arabco.redSea.wells[0];
        } else if (device.hierarchy_name.includes('Well-402')) {
          coordinates = locationData.arabco.redSea.wells[1];
        }
      } else if (device.company_name === 'Saher Flow') {
        // Saher Flow devices
        if (device.hierarchy_name.includes('Demo Well A1')) {
          coordinates = locationData.saherFlow.demoAlpha.wells[0];
        } else if (device.hierarchy_name.includes('Demo Well A2')) {
          coordinates = locationData.saherFlow.demoAlpha.wells[1];
        } else if (device.hierarchy_name.includes('Demo Well B1')) {
          coordinates = locationData.saherFlow.demoBeta.wells[0];
        } else if (device.hierarchy_name.includes('Demo Well B2')) {
          coordinates = locationData.saherFlow.demoBeta.wells[1];
        } else if (device.hierarchy_name.includes('Well-D1')) {
          coordinates = locationData.saherFlow.dubai.wells[0];
        } else if (device.hierarchy_name.includes('Well-D2')) {
          coordinates = locationData.saherFlow.dubai.wells[1];
        } else if (device.hierarchy_name.includes('Well-E1')) {
          coordinates = locationData.saherFlow.cairo.wells[0];
        } else if (device.hierarchy_name.includes('Well-E2')) {
          coordinates = locationData.saherFlow.cairo.wells[1];
        }
      }

      if (coordinates) {
        // Add small random variation to avoid exact overlaps for multiple devices at same well
        const latVariation = (Math.random() - 0.5) * 0.001; // ~100m variation
        const lngVariation = (Math.random() - 0.5) * 0.001;
        
        const finalLat = coordinates.lat + latVariation;
        const finalLng = coordinates.lng + lngVariation;

        // Update device_data table
        await database.query(`
          UPDATE device_data 
          SET longitude = $1, latitude = $2
          WHERE serial_number = $3
        `, [finalLng, finalLat, device.serial_number]);

        // Update device_latest table
        await database.query(`
          UPDATE device_latest 
          SET longitude = $1, latitude = $2
          WHERE serial_number = $3
        `, [finalLng, finalLat, device.serial_number]);

        console.log(`✅ Updated ${device.serial_number} at ${device.hierarchy_name}: ${finalLat.toFixed(4)}, ${finalLng.toFixed(4)}`);
      } else {
        console.log(`⚠️ No coordinates found for ${device.serial_number} at ${device.hierarchy_name}`);
      }
    }

    console.log('\n🗺️ Device location update completed!');
    
    // Show summary of updated locations
    const locationSummary = await database.query(`
      SELECT 
        c.name as company_name,
        COUNT(DISTINCT d.id) as total_devices,
        COUNT(DISTINCT CASE WHEN dl.longitude IS NOT NULL THEN d.id END) as devices_with_location
      FROM device d
      JOIN hierarchy h ON d.hierarchy_id = h.id
      JOIN company c ON h.company_id = c.id
      LEFT JOIN device_latest dl ON d.serial_number = dl.serial_number
      GROUP BY c.name
      ORDER BY c.name
    `);

    console.log('\n📊 Location Update Summary:');
    locationSummary.rows.forEach(row => {
      console.log(`  ${row.company_name}: ${row.devices_with_location}/${row.total_devices} devices have coordinates`);
    });

  } catch (error) {
    console.error('❌ Error updating device locations:', error);
    throw error;
  }
};

module.exports = updateDeviceLocations;

// Run if called directly
if (require.main === module) {
  const runUpdate = async () => {
    try {
      await database.connect();
      await updateDeviceLocations();
    } catch (error) {
      console.error('Failed to update locations:', error);
    } finally {
      await database.disconnect();
      process.exit(0);
    }
  };
  
  runUpdate();
}