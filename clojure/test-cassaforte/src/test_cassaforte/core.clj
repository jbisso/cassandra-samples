(ns test-cassaforte.core 
  (:require [clojurewerkz.cassaforte.client :as client])
  (:require [clojurewerkz.cassaforte.cql :as cql]))

(defn -main
  "Try out the Cassaforte library; it uses the DataStax Java driver."
  [& args]
  (let [
      session (client/connect! "127.0.0.1")
      keyspace (cql/create-keyspace "simplex"
          (with {:replication
              {:class "SimpleStrategy"
              :replication_factor 2 }}))
      ]
      )
  )

