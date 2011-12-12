(ns mbs-db.util)

;;;;;;;;;;;;;;;;;;;;;;;;; simple Vigenère chiffre ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:dynamic *crypt-key* "A")
(defn- crypt-char [offset base c]
  (-> (int c)
    (+ offset)
    (- base)
    (mod 26)
    (+ base)
    char))

(defn- crypt [offsets s]
  (let [a (int \a)
        z (int \z)
        A (int \A)
        Z (int \Z)]
    (apply str (map (fn [offset c]
                      (cond
                        (<= a (int c) z) (crypt-char offset a c)
                        (<= A (int c) Z) (crypt-char offset A c)
                        :else c))
                    offsets s))))
(defn- get-offsets [key]
  (cycle (map #(- (int (Character/toUpperCase %)) (int \A)) key)))

(defn encrypt [s]
  (crypt (get-offsets *crypt-key*) s))

(defn decrypt [s]
  (crypt (map #(- 26 %) (get-offsets *crypt-key*)) s))

(defn encrypt-name [^String s]
  (let [idx (.indexOf s ".")
        idx (if (= -1 idx) (count s) idx)
        name (subs s 0 idx)
        rest (subs s idx)]
    (str (encrypt name) rest)))
(defn decrypt-name [^String s]
  (let [idx (.indexOf s ".")
        idx (if (= -1 idx) (count s) idx)
        name (subs s 0 idx)
        rest (subs s idx)]
    (str (decrypt name) rest)))