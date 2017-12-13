;; https://github.com/jonase/eastwood#eastwood-config-files

;; Disable some clojure.spec warnings

(disable-warning
 {:linter :suspicious-expression
  :for-macro 'clojure.core/and
  :if-inside-macroexpansion-of
  #{'clojure.spec.alpha/keys}})
