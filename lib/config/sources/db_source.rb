require 'yaml'
require 'erb'
require 'corevist_api'

module Config
  module Sources
    class DbSource
      attr_reader :table, :config_line, :keys, :conditions

      def initialize(options)
        @table = options.is_a?(Hash) ? options[:table].to_s : options
        @keys = options.is_a?(Hash) ? options[:keys].to_a.map { |key| "'#{key}'" } : []
        @conditions = options.is_a?(Hash) ? prepare_conditions(options[:conditions]) : []
        @retry_count = 0
      end

      def load
        connection_pool = CorevistAPI::ApplicationRecord.connection_pool
        conn = CorevistAPI::ApplicationRecord.retrieve_connection

        if table && conn && connection_pool.connected? && conn.table_exists?(table)
          file_contents = { table => parse_values(CorevistAPI::ApplicationRecord.connection.execute(_sql).to_h) }
          result = file_contents.with_indifferent_access
        end

        conn.close

        result.presence || {}
      rescue ActiveRecord::NoDatabaseError
        {}
      rescue ActiveRecord::ConnectionNotEstablished => exception
        config = CorevistAPI::ApplicationRecord.configurations.configs_for(env_name: Rails.env, name: 'default')
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

      def _sql
        result = "SELECT `#{Config.key_key}`, `#{Config.value_key}` FROM `#{table}`"
        if !keys.empty?
          result += " WHERE `#{Config.key_key}` IN (#{keys.join(',')})"
          result += " AND #{conditions}" unless conditions.empty?
        else
          result += " WHERE #{conditions}" unless conditions.empty?
        end
        "#{result};"
      end

      def prepare_conditions(conditions)
        conditions.to_h.each_with_object([]) do |(key, value), memo|
          next if value.nil?

          memo << "`#{key}` = #{value.is_a?(Numeric) ? "#{value}" : "'#{value}'" }"
        end.join(' AND ')
      end
    end
  end
end
