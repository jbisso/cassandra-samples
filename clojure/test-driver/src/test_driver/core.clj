(ns test-driver.core
  (:gen-class))

(import
  '(com.datastax.driver.core Cluster))
 
(defn connect-db
    "Connect to the specified node and return the session."
    [node]
    (.connect 
        (.build 
            (.addContactPoint 
                (Cluster/builder)
                node)))
    )
 
(defn do-queries
    "Connects via the specified session and executes the statements."
    [session statements]
    (map 
        (fn [arg]
            (.execute session arg)) 
        statements)
    )

(defn -main
    "Test the DataStax Java driver for Cassandra in Clojure."
    [& args]
    (let [
        session (connect-db "127.0.0.1")
        results (do-queries
            session
            (list
                "CREATE KEYSPACE simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};"
                "CREATE TABLE simplex.users (userid text PRIMARY KEY, passwd text);"
                "INSERT INTO simplex.users (userid, passwd) VALUES ('jot', 'tittle');"
                "SELECT * FROM simplex.users;")
            )
        queries (map 
            (fn [rs] 
                (mapcat
                    (fn [r]
                        (list (.getString r "userid") (.getString r "passwd")))
                    (.all rs)))
            results)]
        ;;(.execute session "DROP KEYSPACE simplex;")
        (.shutdown (.getCluster session))
        (.shutdown session)
        queries
        )
    )


