module RR

  # Shared functionality for DirectTableScan and ProxiedTableScan
  class TableScan
    include TableScanHelper

    # The current Session object
    attr_accessor :session 
      
    # Name of the left table
    attr_accessor :left_table
    
    # Name of the right table
    attr_accessor :right_table

    # Cached array of primary key names
    attr_accessor :primary_key_names

    # Receives the active ScanProgressPrinters class
    attr_accessor :progress_printer

    # Inform new progress to progress printer
    # +steps+ is the number of processed records.
    def update_progress(steps)
      return unless progress_printer
      unless @progress_printer_instance
        total_records =
          session.left.select_one("select count(*) as n from #{left_table}")['n'].to_i +
          session.right.select_one("select count(*) as n from #{right_table}")['n'].to_i
        @progress_printer_instance = progress_printer.new(total_records, session, left_table, right_table)
      end
      @progress_printer_instance.step(steps)
    end
    
    # Creates a new DirectTableScan instance
    #   * session: a Session object representing the current database session
    #   * left_table: name of the table in the left database
    #   * right_table: name of the table in the right database. If not given, same like left_table
    def initialize(session, left_table, right_table = nil)
      if session.left.primary_key_names(left_table).empty?
        raise "Table #{left_table} doesn't have a primary key. Cannot scan."
      end
      
      self.session, self.left_table, self.right_table = session, left_table, right_table
      self.right_table ||= self.left_table
      self.primary_key_names = session.left.primary_key_names left_table
    end
  end
end
