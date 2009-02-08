module RR
  module ScanProgressPrinters
    
    # A helper class to print a text progress bar.
    class ProgressBar

      MAX_MARKERS = 25 #length of the progress bar (in characters)

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
        @max_markers ||= arg ? arg.to_i : MAX_MARKERS
      end

      # Creates a new progress bar.
      # * +max_steps+: number of steps at completion
      # * +session+: the current Session
      # * +left_table+: name of the left database table
      # * +right_table+: name of the right database table
      def initialize(max_steps, session, left_table, right_table)
        @use_ansi = session.configuration.options_for_table(left_table)[:use_ansi]
        @max_steps, @current_steps = max_steps, 0
        @steps_per_marker = @max_steps.to_f / max_markers
        @current_markers, @current_percentage = 0, 0
      end
  
      # Increases progress by +step_increment+ steps.
      def step(step_increment = 1)
        @current_steps+= step_increment
        new_markers = @max_steps != 0 ? (@current_steps / @steps_per_marker).to_i : max_markers

        new_percentage = @max_steps != 0 ? @current_steps * 100 / @max_steps : 100
        if @use_ansi and new_percentage != @current_percentage
          # This part uses ANSI escape sequences to show a running percentage
          # to the left of the progress bar
          print "\e[1D" * (@current_markers + 5) if @current_percentage != 0 # go left
          print "#{new_percentage}%".rjust(4) << " "
          print "\e[1C" * @current_markers if @current_markers != 0 # go back right
          $stdout.flush
          @current_percentage = new_percentage
        end

        if new_markers > @current_markers
          print '.'  * (new_markers - @current_markers)
          @current_markers = new_markers
          $stdout.flush
        end
        if @current_steps == @max_steps
          print '.' * (max_markers - @current_markers) + ' '
          $stdout.flush
        end
      end
    end
  end
end