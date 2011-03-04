$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib/rubyrep'
#require 'rubyrep/version'

AUTHOR = 'Arndt Lehmann'  # can also be an array of Authors
EMAIL = "mail@arndtlehman.com"
DESCRIPTION = "Asynchronous master-master replication of relational databases."
GEM_NAME = 'rubyrep' # what ppl will type to install your gem
RUBYFORGE_PROJECT = 'rubyrep' # The unix name for your project
HOMEPATH = "http://#{RUBYFORGE_PROJECT}.rubyforge.org"
DOWNLOAD_PATH = "http://rubyforge.org/projects/#{RUBYFORGE_PROJECT}"

@config_file = "~/.rubyforge/user-config.yml"
@config = nil
RUBYFORGE_USERNAME = "alehmann"
def rubyforge_username
  unless @config
    begin
      @config = YAML.load(File.read(File.expand_path(@config_file)))
    rescue
      puts <<-EOS
ERROR: No rubyforge config file found: #{@config_file}
Run 'rubyforge setup' to prepare your env for access to Rubyforge
 - See http://newgem.rubyforge.org/rubyforge.html for more details
      EOS
      exit
    end
  end
  RUBYFORGE_USERNAME.replace @config["username"]
end


REV = nil 
# UNCOMMENT IF REQUIRED: 
# REV = `svn info`.each {|line| if line =~ /^Revision:/ then k,v = line.split(': '); break v.chomp; else next; end} rescue nil
VERS = RR::VERSION::STRING + (REV ? ".#{REV}" : "")

ENV['RDOCOPT'] = "-S -f html -T hanna"

class Hoe
  def extra_deps 
    @extra_deps.reject! { |x| Array(x).first == 'hoe' } 
    @extra_deps
  end 
end

# Generate all the Rake tasks
# Run 'rake -T' to see list of generated tasks (from gem root directory)
hoe = Hoe.spec(GEM_NAME) do
  self.version = VERS
  developer AUTHOR, EMAIL
  description = DESCRIPTION
  summary = DESCRIPTION
  url = HOMEPATH
  rubyforge_name = RUBYFORGE_PROJECT if RUBYFORGE_PROJECT
  test_globs = ["test/**/test_*.rb"]
  clean_globs |= ['**/.*.sw?', '*.gem', '.config', '**/.DS_Store']  #An array of file patterns to delete on clean.

  # == Optional
  changes = paragraphs_of("History.txt", 0..1).join("\\n\\n")
  #extra_deps = []     # An array of rubygem dependencies [name, version], e.g. [ ['active_support', '>= 1.3.1'] ]
  extra_deps << ['activesupport', '>= 3.0.5']
  extra_deps << ['activerecord' , '>= 3.0.5']
  
  #spec_extras = {}    # A hash of extra values to set in the gemspec.
  
end

CHANGES = hoe.paragraphs_of('History.txt', 0..1).join("\\n\\n")
PATH    = (RUBYFORGE_PROJECT == GEM_NAME) ? RUBYFORGE_PROJECT : "#{RUBYFORGE_PROJECT}/#{GEM_NAME}"
hoe.remote_rdoc_dir = File.join(PATH.gsub(/^#{RUBYFORGE_PROJECT}\/?/,''))
