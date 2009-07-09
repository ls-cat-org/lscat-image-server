#include "is.h"

PGconn *db=NULL;

void dbInit() {
  PGresult* res;
  int status;

  if( db == NULL) {
    /* open up the connection. */
    db = PQconnectdb("dbname=ls user=lsuser host=10.1.0.3");
    
    status = PQstatus( db);
    if( status != CONNECTION_OK) {
      fprintf( stderr, "dbInit: Failed to connect to database\n");
      return;
    }

    /** get the next pid. */
    res = PQexec( db, "select rmt.isInit()");

    status = PQstatus( db);
    if( status == PGRES_BAD_RESPONSE || status == PGRES_FATAL_ERROR || status == PGRES_NONFATAL_ERROR) {
      fprintf( stderr, "dbInit: Initialization failed (%d)\n", status);
      return;
    }
  }
  return;
}


void dbWait() {
  struct pollfd pfd;
  int pstat;
  PGnotify *pn;

  pfd.fd     = PQsocket( db);
  pfd.events = POLLIN;

  // See if anything is up
  pn = PQnotifies( db);

  if( pn != NULL) {
    // got a notify (free it)
    PQfreemem( pn);
  } else {
    // wait for a notify
    pstat = poll( &pfd, 1, -1);
  }

  // got a notify (probably)
  pn = PQnotifies( db);
  if( pn != NULL) {
    PQfreemem( pn);
  }
}

int dbGet( isType *inf) {
  int rtn;		// return 0 if nothing was got, 1 otherwise
  PGresult *res;	// query result
  int status;		// status of query

  rtn = 0;	// ignore result by default

  res = PQexec( db, "select * from rmt.popIs() where isuser is not null");
  if( res == NULL)
    return rtn;
  
  status = PQresultStatus( res);
  if( status != PGRES_TUPLES_OK) {
    fprintf( stderr, "popIs failure: %s\n %s\n", PQresStatus( status), PQresultErrorMessage( res));
    return rtn;
  }

  rtn = PQntuples( res);
  if( rtn != 1)
    return 0;		// more than one row is bad, treat it as though it didn't find any

  // usually we'll get a row nulls

  {
    PQprintOpt zz;
    zz.header = 1;
    zz.align  = 1;
    zz.standard = 0;
    zz.html3    = 0;
    zz.expanded = 0;
    zz.pager    = 0;
    zz.fieldSep = "|";
    zz.tableOpt = "";
    zz.caption  = "";
    zz.fieldName = NULL;

    PQprint( stderr, res, &zz);
  }
  return rtn;
}
