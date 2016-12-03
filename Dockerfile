FROM clojure:lein-2.7.1-onbuild
ADD . /code
ADD . /dbdata
WORKDIR /code
RUN lein install
CMD lein repl
