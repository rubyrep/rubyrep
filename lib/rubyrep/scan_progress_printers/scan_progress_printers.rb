module RR
  # Manages scan progress printers. Scan progress printers implement functionality
  # to report the progress of a table scan.
  # *Each* table scan is handled by a *separate* printer instance.
  #
  # Scan progress printers need to register themselves and their command line options
  # with #register.
  #
  # A scan progress printer needs to implement at the minimum the following
  # functionality:
  #
  #   # Receives the command line argument as yielded by OptionParser#on.
  #   def self.arg=(arg)
  #
  #   # Creation of a new ScanProgressPrinter.
  #   # * +max_steps+: number of steps at completion
  #   # * +session+: the current Session
  #   # * +left_table+: name of the left database table
  #   # * +right_table+: name of the right database table
  #   def initialize(max_steps, left_table, right_table)
  #
  #   # Progress is advanced by +progress+ number of steps.
  #   def step(progress)
  #
  module ScanProgressPrinters

    # Hash of registered ScanProgressPrinters.
    # Each entry is a hash with the following key and related value:
    # * key: Identifier of the progress printer
    # * value: A hash with payload information. Possible values:
    #   * :+printer_class+: The ScanProgressPrinter class.
    #   * :+opts+: An array defining the command line options (handed to OptionParter#on).
    def self.printers
      @@progress_printers ||= {}
    end

    # Needs to be called by ScanProgressPrinters to register themselves (+printer+)
    # and their command line options.
    # * :+printer_id+ is the symbol through which the printer can be referenced.
    # * :+printer_class+ is the ScanProgressPrinter class,
    # * :+opts+ is an array defining the command line options (handed to OptionParter#on).
    def self.register(printer_id, printer_class, *opts)
      printers[printer_id] = {
        :printer_class => printer_class,
        :opts => opts
      }
    end

    # Registers all report printer command line options into the given
    # OptionParser.
    # Once the command line is parsed with OptionParser#parse! it will
    # yield the correct printer class.
    #
    # Note:
    # If multiple printers are specified in the command line, all are yielded.
    def self.on_printer_selection(opts)
      printers.each_value do |printer|
        opts.on(*printer[:opts]) do |arg|
          printer[:printer_class].arg = arg
          yield printer[:printer_class]
        end
      end
    end
  end
end