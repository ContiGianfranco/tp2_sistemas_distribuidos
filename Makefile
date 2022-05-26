SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./student_comment/student_comment.dockerfile -t "student_comment:latest" .
	docker build -f ./post_join_dispatch/post_join_dispatch.dockerfile -t "post_join_dispatch:latest" .
	docker build -f ./join_avg/join_avg.dockerfile -t "join_avg:latest" .
	docker build -f ./top_sentiment/top_sentiment.dockerfile -t "top_sentiment:latest" .
	docker build -f ./sentiment_adder/sentiment_adder.dockerfile -t "sentiment_adder:latest" .
	docker build -f ./joiner/joiner.dockerfile -t "joiner:latest" .
	docker build -f ./avg/avg.dockerfile -t "avg:latest" .
	docker build -f ./rabbitmq/rabbitmq.dockerfile -t "rabbitmq:latest" .
	docker build -f ./comments_receiver/comments_receiver.dockerfile -t "comments_receiver:latest" .
	docker build -f ./adder/adder.dockerfile -t "adder:latest" .
	docker build -f ./post_receiver/post_receiver.dockerfile -t "post_receiver:latest" .
	docker build -f ./client/client.dockerfile -t "client:latest" .
	# Execute this command from time to time to clean up intermediate stages generated
	# during client build (your hard drive will like this :) ). Don't left uncommented if you
	# want to avoid rebuilding client image every time the docker-compose-up command
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose-dev.yaml stop -t 20
	docker-compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
