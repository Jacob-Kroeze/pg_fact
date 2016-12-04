FROM clojure:lein-2.7.1-onbuild
ADD . /code
WORKDIR /code
RUN lein install
CMD lein run

