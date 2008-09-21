module RR
  module ScanProgressPrinters
    
    # A helper class to print a text progress bar.
    class ProgressBar

      MAX_MARKERS = 40 #length of the progress bar (in characters)

      # Register ProgressBar with the given command line options.
      # (Command line format as specified by OptionParser#on.)
      # First argument is the key through which the printer can be refered in
      # the configuration file
      RR::ScanProgressPrinters.register :progress_bar, self,
        "-b", "--progress-bar[=length]",
        "Show the progress of the table scanning process as progress bar."

      # Receives the command line argument
      cattr_accessor :arg

      # Returns the length (in characters) of the progress bar.
      def max_markers
        @max_marker ||= arg ? arg.to_i : MAX_MARKERS
      end

      # Creates a new progress bar for a task consisting of +max+ steps.
      # +left_table+ and +right_table+ are the names of the tables that are
      # getting processed
      def initialize(max_steps, left_table, right_table)
        @max_steps, @current_steps = max_steps, 0
        @max_markers = MAX_MARKERS
        @steps_per_marker = @max_steps.to_f / @max_markers
        @current_markers = 0
        puts "\nScanning #{left_table}, #{right_table}"
        puts "0%>#{'-' * (@max_markers - '0%>'.length - '100%>'.length)}>100%"
      end
  
      # Increases progress by +step_increment+ steps.
      def step(step_increment = 1)
        @current_steps+= step_increment
        new_markers = (@current_steps / @steps_per_marker).to_i
        if new_markers > @current_markers
          print '.'  * (new_markers - @current_markers)
          @current_markers = new_markers
          puts if @current_markers == @max_markers
          $stdout.flush
        end
      end
    end
  end
end