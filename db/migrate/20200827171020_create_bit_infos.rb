class CreateBitInfos < ActiveRecord::Migration[6.0]
  def change
    create_table :bit_infos, primary_key: ["seq"] do |t|
      t.integer :seq
      t.integer :auc_seq
      t.date :bit_time
      t.integer :bit_price
      t.integer :bit_member
      t.integer :bit_snum
      t.integer :is_senko_bit

      t.timestamps
    end
    add_index :bit_infos, :seq, unique: true
  end
end
