const { Pool } = require('pg');

class Database {
  constructor() {
    this.pool = null;
  }

  async connect() {
    try {
      if (!process.env.DATABASE_URL) {
        throw new Error('DATABASE_URL environment variable is not defined');
      }

      this.pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
      });

      // Test connection
      const client = await this.pool.connect();
      console.log('Connected to PostgreSQL');
      client.release();

      // Initialize database schema
      //await this.initializeSchema();
    } catch (error) {
      console.error('Database connection error:', error.message || error);
      console.log('\n📋 To fix this error:');
      console.log('1. Set up a PostgreSQL database');
      console.log('2. Set DATABASE_URL in your .env file');
      console.log('3. Restart the server');
      throw error;
    }
  }

  async initializeSchema() {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Create company table
      await client.query(`
        CREATE TABLE IF NOT EXISTS company (
          id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          domain_name TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      // Create user table
      await client.query(`
        CREATE TABLE IF NOT EXISTS "user" (
          id BIGSERIAL PRIMARY KEY,
          company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
          name TEXT NOT NULL,
          email TEXT UNIQUE NOT NULL,
          is_email_verified BOOLEAN DEFAULT FALSE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ,
          is_active BOOLEAN DEFAULT TRUE,
          last_login_time TIMESTAMPTZ,
          last_login_ip INET,
          allowed_ip_list INET[],
          password_hash TEXT,
          role TEXT DEFAULT 'user',
          email_verification_token TEXT,
          email_verification_expires TIMESTAMPTZ,
          password_reset_token TEXT,
          password_reset_expires TIMESTAMPTZ,
          email_validated BOOLEAN DEFAULT FALSE
        )
      `);

      // Create device_type table
      await client.query(`
        CREATE TABLE IF NOT EXISTS device_type (
          id BIGSERIAL PRIMARY KEY,
          type_name TEXT NOT NULL UNIQUE,
          logo TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      // Create hierarchy_level table
      await client.query(`
        CREATE TABLE IF NOT EXISTS hierarchy_level (
          id BIGSERIAL PRIMARY KEY,
          level_order INTEGER NOT NULL UNIQUE,
          name TEXT NOT NULL,
          icon TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      // Create hierarchy table
      await client.query(`
        CREATE TABLE IF NOT EXISTS hierarchy (
          id BIGSERIAL PRIMARY KEY,
          company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
          name TEXT NOT NULL,
          level_id BIGINT NOT NULL REFERENCES hierarchy_level(id),
          parent_id BIGINT REFERENCES hierarchy(id) ON DELETE CASCADE,
          can_attach_device BOOLEAN DEFAULT TRUE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ
        )
      `);

      // Create device table
      await client.query(`
        CREATE TABLE IF NOT EXISTS device (
          id BIGSERIAL PRIMARY KEY,
          company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
          hierarchy_id BIGINT REFERENCES hierarchy(id) ON DELETE SET NULL,
          device_type_id BIGINT NOT NULL REFERENCES device_type(id),
          serial_number TEXT NOT NULL UNIQUE,
          metadata JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      // Create device_data table
      await client.query(`
        CREATE TABLE IF NOT EXISTS device_data (
          id BIGSERIAL PRIMARY KEY,
          device_id BIGINT NOT NULL REFERENCES device(id) ON DELETE CASCADE,
          serial_number TEXT NOT NULL REFERENCES device(serial_number),
          created_at TIMESTAMPTZ NOT NULL,
          longitude DOUBLE PRECISION,
          latitude DOUBLE PRECISION,
          data JSONB NOT NULL
        )
      `);

      // Create device_latest table
      await client.query(`
        CREATE TABLE IF NOT EXISTS device_latest (
          device_id BIGINT PRIMARY KEY REFERENCES device(id) ON DELETE CASCADE,
          serial_number TEXT NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL,
          longitude DOUBLE PRECISION,
          latitude DOUBLE PRECISION,
          data JSONB NOT NULL,
          received_at TIMESTAMPTZ DEFAULT now() NOT NULL
        )
      `);

      // Create alarm_types table
      await client.query(`
        CREATE TABLE IF NOT EXISTS alarm_types (
          id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          severity TEXT NOT NULL CHECK (severity IN ('Critical', 'Major', 'Minor', 'Warning')),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      // Create alarm_status_type table
      await client.query(`
        CREATE TABLE IF NOT EXISTS alarm_status_type (
          id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          description TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      // Create device_alarms table
      await client.query(`
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

      // Create device_data_mapping table
      await client.query(`
        CREATE TABLE IF NOT EXISTS device_data_mapping (
          id BIGSERIAL PRIMARY KEY,
          device_type_id BIGINT NOT NULL REFERENCES device_type(id) ON DELETE CASCADE,
          variable_name TEXT NOT NULL,
          variable_tag TEXT,
          data_type TEXT,
          unit TEXT,
          ui_order INTEGER DEFAULT 100,
          expression TEXT,
          requires_post_aggregation BOOLEAN DEFAULT FALSE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      // Create user_hierarchy table
      await client.query(`
        CREATE TABLE IF NOT EXISTS user_hierarchy (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
          hierarchy_id BIGINT NOT NULL REFERENCES hierarchy(id) ON DELETE CASCADE,
          role TEXT DEFAULT 'viewer',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      await client.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS widget_types (
          id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          name VARCHAR(100) NOT NULL UNIQUE,
          component_name VARCHAR(100) NOT NULL,
          default_config JSONB NOT NULL DEFAULT '{}',
          created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS widget_definitions (
          id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          name VARCHAR(200) NOT NULL,
          description TEXT,
          widget_type_id UUID NOT NULL REFERENCES widget_types(id) ON DELETE RESTRICT,
          data_source_config JSONB NOT NULL DEFAULT '{}',
          layout_config JSONB NOT NULL DEFAULT '{}',
          created_by BIGINT NOT NULL REFERENCES "user"(id) ON DELETE RESTRICT,
          created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS dashboards (
          id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          name VARCHAR(200) NOT NULL,
          description TEXT,
          version INTEGER DEFAULT 1,
          is_active BOOLEAN DEFAULT TRUE,
          grid_config JSONB NOT NULL DEFAULT '{"cols": 12, "rowHeight": 100, "margin": [10, 10], "breakpoints": {"lg": 1200, "md": 996, "sm": 768, "xs": 480, "xxs": 0}, "containerPadding": [10, 10]}',
          created_by BIGINT NOT NULL REFERENCES "user"(id) ON DELETE RESTRICT,
          created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS dashboard_layouts (
          id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          dashboard_id UUID NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
          widget_definition_id UUID NOT NULL REFERENCES widget_definitions(id) ON DELETE CASCADE,
          layout_config JSONB NOT NULL DEFAULT '{"x": 0, "y": 0, "w": 4, "h": 2, "minW": 2, "minH": 1, "static": false}',
          instance_config JSONB NOT NULL DEFAULT '{}',
          display_order INTEGER DEFAULT 0,
          created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(dashboard_id, widget_definition_id)
        )
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS dashboard_shares (
          id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          dashboard_id UUID NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
          user_id BIGINT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
          permission_level VARCHAR(20) NOT NULL DEFAULT 'view',
          shared_by BIGINT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
          shared_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
          expires_at TIMESTAMPTZ,
          UNIQUE(dashboard_id, user_id),
          CHECK (permission_level IN ('view', 'edit', 'admin'))
        )
      `);

      await client.query(`
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
          NEW.updated_at = CURRENT_TIMESTAMP;
          RETURN NEW;
        END;
        $$ language 'plpgsql'
      `);

      await client.query(`
        DROP TRIGGER IF EXISTS update_widget_definitions_updated_at ON widget_definitions;
        CREATE TRIGGER update_widget_definitions_updated_at
          BEFORE UPDATE ON widget_definitions
          FOR EACH ROW
          EXECUTE FUNCTION update_updated_at_column()
      `);

      await client.query(`
        DROP TRIGGER IF EXISTS update_dashboards_updated_at ON dashboards;
        CREATE TRIGGER update_dashboards_updated_at
          BEFORE UPDATE ON dashboards
          FOR EACH ROW
          EXECUTE FUNCTION update_updated_at_column()
      `);

      await client.query(`
        DROP TRIGGER IF EXISTS update_dashboard_layouts_updated_at ON dashboard_layouts;
        CREATE TRIGGER update_dashboard_layouts_updated_at
          BEFORE UPDATE ON dashboard_layouts
          FOR EACH ROW
          EXECUTE FUNCTION update_updated_at_column()
      `);

      // Create indexes
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_user_email ON "user"(email);
        CREATE INDEX IF NOT EXISTS idx_user_company_id ON "user"(company_id);
        CREATE INDEX IF NOT EXISTS idx_company_domain ON company(domain_name);
        CREATE INDEX IF NOT EXISTS idx_hierarchy_company_id ON hierarchy(company_id);
        CREATE INDEX IF NOT EXISTS idx_hierarchy_parent_id ON hierarchy(parent_id);
        CREATE INDEX IF NOT EXISTS idx_hierarchy_level_id ON hierarchy(level_id);
        CREATE INDEX IF NOT EXISTS idx_device_company_id ON device(company_id);
        CREATE INDEX IF NOT EXISTS idx_device_hierarchy_id ON device(hierarchy_id);
        CREATE INDEX IF NOT EXISTS idx_device_serial ON device(serial_number);
        CREATE INDEX IF NOT EXISTS idx_device_data_device_created_at ON device_data(device_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS brin_device_data_created_at ON device_data USING brin(created_at);
        CREATE INDEX IF NOT EXISTS gin_device_data_data ON device_data USING gin(data);
        CREATE INDEX IF NOT EXISTS fk_device_serial ON device_data(serial_number);
        CREATE INDEX IF NOT EXISTS idx_device_alarms_device_serial ON device_alarms(device_serial);
        CREATE INDEX IF NOT EXISTS idx_device_alarms_alarm_type_id ON device_alarms(alarm_type_id);
        CREATE INDEX IF NOT EXISTS idx_device_alarms_status_id ON device_alarms(status_id);
        CREATE INDEX IF NOT EXISTS idx_device_alarms_created_at ON device_alarms(created_at DESC);
        CREATE INDEX IF NOT EXISTS gin_device_alarms_metadata ON device_alarms USING gin(metadata);
        CREATE INDEX IF NOT EXISTS idx_widget_definitions_created_by ON widget_definitions(created_by);
        CREATE INDEX IF NOT EXISTS idx_widget_definitions_widget_type_id ON widget_definitions(widget_type_id);
        CREATE INDEX IF NOT EXISTS idx_dashboards_created_by ON dashboards(created_by);
        CREATE INDEX IF NOT EXISTS idx_dashboards_is_active ON dashboards(is_active);
        CREATE INDEX IF NOT EXISTS idx_dashboard_layouts_dashboard_id ON dashboard_layouts(dashboard_id);
        CREATE INDEX IF NOT EXISTS idx_dashboard_layouts_widget_def_id ON dashboard_layouts(widget_definition_id);
        CREATE INDEX IF NOT EXISTS idx_dashboard_shares_dashboard_id ON dashboard_shares(dashboard_id);
        CREATE INDEX IF NOT EXISTS idx_dashboard_shares_user_id ON dashboard_shares(user_id);
      `);

      await client.query('COMMIT');
      console.log('Database schema initialized');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async query(text, params) {
    return this.pool.query(text, params);
  }

  async transaction(callback) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async disconnect() {
    if (this.pool) {
      await this.pool.end();
      console.log('Database disconnected');
    }
  }
}

module.exports = new Database();