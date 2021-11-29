require 'yaml'
require 'erb'

module Config
  module Sources
    class DbSource
      attr_reader :table, :config_line, :db_config

      def initialize(table)
        @table = table.to_s
        @db_config = File.read(Config.database_yml_path)
        @retry_count = 0
      end

      def load
        connection_pool = ActiveRecord::Base.connection_pool
        connection_pool.lock_thread = true
        conn = ActiveRecord::Base.retrieve_connection

        if table && conn && connection_pool.connected? && conn.table_exists?(table)
          sql = "select `#{Config.key_key}`, `#{Config.value_key}` from #{table};"
          file_contents = { table => parse_values(ActiveRecord::Base.connection.execute(sql).to_h) }
          result = file_contents.with_indifferent_access
        end

        connection_pool.connection.close if connection_pool.active_connection?
        connection_pool.disconnect! if connection_pool.connected?

        result.presence || {}
      rescue ActiveRecord::NoDatabaseError
        {}
      rescue ActiveRecord::ConnectionNotEstablished => exception
        config = YAML.load(db_config)[Rails.env]
        ActiveRecord::Base.establish_connection(config)
        @retry_count += 1 and retry if @retry_count < 1

        raise exception
      end

      def parse_values(file_contents)
        return file_contents if file_contents.blank?

        file_contents.each { |k, v| (@config_line = k) && (file_contents[k] = YAML.load(v)) }
      rescue Psych::SyntaxError => e
        raise "YAML syntax error occurred while parsing config item #{config_line}. " \
                "Please note that YAML must be consistently indented using spaces. Tabs are not allowed. " \
                "Error: #{e.message}"
      end
    end
  end
end
