# A helper class to print a text progress bar.
class ProgressBar
  # Creates a new progress bar for a task consisting of +max+ steps.
  # At 100% the progress bar will be +bar_length+ characters wide.
  def initialize(max_steps, bar_length = 40)
    @max_steps, @bar_length, @current_progress = max_steps, bar_length, 0
    @steps_per_progress_bar_marker = @max_steps.to_f / @bar_length
    @marker_counter = 0
    puts "0%>#{'-' * (@bar_length - '0%>'.length - '100%>'.length)}>100%"
  end
  
  # Increases progress by +number_steps+ steps.
  # If no argument provided, increase progress by 1 step.
  def step(number_steps = 1)
    @current_progress+= number_steps
    if ((@current_progress  - number_steps) / @steps_per_progress_bar_marker).to_i \
        < (@current_progress / @steps_per_progress_bar_marker).to_i
      @marker_counter += 1
      putc '.' 
      puts if @marker_counter == @bar_length # after the last marker is printed, add a new_line
      $stdout.flush
    end
  end
end