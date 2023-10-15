package rest

import (
	"events/src/lib/msgqueue"
	"events/src/lib/persistence"
	"net/http"

	"github.com/gorilla/mux"
)

func ServeAPI(endpoint string, dbHandler persistence.DatabaseHandler, eventEmitter msgqueue.EventEmitter) error {
	handler := newEventHandler(dbHandler, eventEmitter)

	r := mux.NewRouter()
	eventsrouter := r.PathPrefix("/events").Subrouter()
	eventsrouter.Methods("GET").Path("/{SearchCriteria}/{search}").HandlerFunc(handler.findEventHandler)
	eventsrouter.Methods("GET").Path("").HandlerFunc(handler.allEventHandler)
	eventsrouter.Methods("GET").Path("/{eventID}").HandlerFunc(handler.oneEventHandler)
	eventsrouter.Methods("POST").Path("").HandlerFunc(handler.newEventHandler)

	locationRouter := r.PathPrefix("/events").Subrouter()
	locationRouter.Methods("GET").Path("").HandlerFunc(handler.allLocationsHandler)
	locationRouter.Methods("POST").Path("").HandlerFunc(handler.newLocationHandler)

	return http.ListenAndServe(endpoint, r)
}
