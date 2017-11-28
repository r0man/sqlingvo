(ns sqlingvo.spec
  (:require [clojure.spec.alpha :as s]))

(s/def ::identifier keyword?)

(s/def ::keyword-identifer keyword?)

;; Schema

(s/def :sqlingvo.schema/name ::keyword-identifer)

;; Alias

(s/def :sqlingvo.alias/op #{:alias})

(s/def :sqlingvo/alias
  (s/keys :req-un [:sqlingvo.alias/op]))

;; Table

(s/def :sqlingvo.table/name ::keyword-identifer)
(s/def :sqlingvo.table/op #{:table})
(s/def :sqlingvo.table/schema :sqlingvo.schema/name)

(s/def :sqlingvo.table/identifier
  (s/or :alias :sqlingvo/alias
        :keyword keyword?
        :string string?
        :table :sqlingvo/table))

(s/def :sqlingvo/table
  (s/keys :req-un [:sqlingvo.table/op
                   :sqlingvo.table/name]
          :opt-un [:sqlingvo.table/schema]))

;; Column

(s/def :sqlingvo.column/name ::keyword-identifer)
(s/def :sqlingvo.column/op #{:column})
(s/def :sqlingvo.column/schema :sqlingvo.schema/name)
(s/def :sqlingvo.column/table :sqlingvo.table/name)

(s/def :sqlingvo.column/identifier
  (s/or :alias :sqlingvo/alias
        :keyword keyword?
        :string string?
        :table :sqlingvo/column))

(s/def :sqlingvo/column
  (s/keys :req-un [:sqlingvo.column/op
                   :sqlingvo.column/name]
          :opt-un [:sqlingvo.column/schema
                   :sqlingvo.column/table]))
