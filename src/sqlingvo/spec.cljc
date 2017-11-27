(ns sqlingvo.spec
  (:require [clojure.spec.alpha :as s]
            [sqlingvo.expr :as expr]))

(defn- ex-invalid
  [spec data]
  (ex-info (str "Can't conform data to spec: " spec "\n\n"
                (with-out-str (s/explain spec data)))
           (s/explain-data spec data)))

(defn conform!
  "Like `clojure.spec.alpha/conform`, but raises an exception if `data`
  doesn't conform to `spec`."
  [spec data]
  (let [conformed (s/conform spec data)]
    (if (s/invalid? conformed)
      (throw (ex-invalid spec data))
      conformed)))

(s/def ::not-empty-string (s/and string? not-empty))

(s/def ::column-name ::not-empty-string)
(s/def ::table-name ::not-empty-string)
(s/def ::table-schema ::not-empty-string)

;; Conformer

(defn- column-conformer [x]
  (or (expr/parse-column x) ::s/invalid))

(defn- table-conformer [x]
  (or (expr/parse-table x) ::s/invalid))

;; Column

(s/def :sqlingvo.column/name ::column-name)
(s/def :sqlingvo.column/schema ::table-schema)
(s/def :sqlingvo.column/table ::table-name)

(s/def :sqlingvo/column
  (s/keys :req [:sqlingvo.column/name]
          :opt [:sqlingvo.column/schema
                :sqlingvo.column/table]))

;; Identifier

(defn- identifier [spec conformer]
  (s/and keyword? (s/conformer conformer) spec))

(s/def :sqlingvo.column/identifier
  (identifier :sqlingvo/column column-conformer))

(s/def :sqlingvo.table/identifier
  (identifier :sqlingvo/table table-conformer))

;; Table

(s/def :sqlingvo.table/name ::table-name)
(s/def :sqlingvo.table/schema ::table-schema)

(s/def :sqlingvo/table
  (s/keys :req [:sqlingvo.table/name]
          :opt [:sqlingvo.table/schema]))
