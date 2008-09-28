# The standard ActiveRecord#create method ignores primary key attributes.
# This module provides a create method that allows manual setting of primary key values.
module CreateWithKey
  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods
    # The standard "create" method ignores primary key attributes
    # This method set's _all_ attributes as provided
    def create_with_key attributes
      o = new
      attributes.each do |key, value|
        o[key] = value
      end
      o.save
    end
  end
end

