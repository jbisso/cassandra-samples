#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clients import BoundStatementsClient

from uuid import UUID

class ByPositionAndByName(BoundStatementsClient):
   def load_data_bound_by_position(self):
      self.insert_user_prepared_statement = self.session.prepare(
         """
            INSERT INTO simplex.songs (id, title, album, artist)
            VALUES (?, ?, ?, ?);
         """)
      # bind parameters by position
      self.session.execute(self.insert_user_prepared_statement,
         [ UUID("756716f7-2e54-4715-9f00-91dcbea6cf50"),
           "Lazing on a Sunday Afternoon",
           "A Night at the Opera",
           "Queen"
         ]
      )

   def load_data_bound_by_name(self):
      self.insert_user_prepared_statement = self.session.prepare(
         """
            INSERT INTO simplex.songs (id, title, album, artist)
               VALUES (:id, :title, :album, :artist);
         """)
      # bind parameters by name
      bound_statement = self.insert_user_prepared_statement.bind(
         { 'id' : UUID('e0e03270-1e6f-11e4-8c21-0800200c9a66'), 
           'title' : 'Coconut', 
           'album' : 'Nilsson Schmilsson',
           'artist' : 'Harry Nilsson' }
      )
      self.session.execute(bound_statement)

def main():
    client = ByPositionAndByName()
    client.connect(['127.0.0.1'])
    client.create_schema()
    client.load_data_bound_by_position()
    client.load_data_bound_by_name()
    client.pause()
    client.drop_schema('simplex')
    client.close()

if __name__ == "__main__":
    main()

