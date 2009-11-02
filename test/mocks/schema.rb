ActiveRecord::Schema.define do

  create_table "locks", :force => true do |t|
    t.string   "name"
  end
  
  add_index "locks", ["name"], :name => "index_locks_on_name", :unique => true

end