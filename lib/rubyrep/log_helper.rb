module RR

  # Shared functionality for SyncHelper and LogHelper
  module LogHelper

    # Takes outcome and details and makes them fit (for available space) in the
    # 'descrition' and 'long_description' columns of the event log.
    # Parameters:
    # * outcome: short description
    # * details: long description
    # Returns (cut off if necessary)
    # * outcome
    # * details (also containig the full outcome if it had to be cut off for short description)
    def fit_description_columns(outcome, details)
      outcome = outcome.to_s
      if outcome.length > ReplicationInitializer::DESCRIPTION_SIZE
        fitting_outcome = outcome[0...ReplicationInitializer::DESCRIPTION_SIZE]
        fitting_details = outcome + "\n"
      else
        fitting_outcome = outcome
        fitting_details = ""
      end
      fitting_details += details if details
      fitting_details = fitting_details[0...ReplicationInitializer::LONG_DESCRIPTION_SIZE]
      fitting_details = nil if fitting_details.empty?

      return fitting_outcome, fitting_details
    end
  end
end