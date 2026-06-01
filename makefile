.PHONY: migrate-up migrate-down migrate-create

migrate-up:
	migrate -path ./migrations -database ${DATABASE_URL} up
migrate-down:
	migrate -path ./migrations -database ${DATABASE_URL} down 1
migrate-create:
	migrate create -ext sql -dir migrations -seq $(name)


