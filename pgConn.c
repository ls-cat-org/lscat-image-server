#include "is.h"

PGconn *db=NULL;
PGresult *isResult = NULL;

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
    PQclear( res);

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

  //
  // Query the database to find our information
  //
  res = PQexec( db, "select * from rmt.popIs() where isuser is not null");
  if( res == NULL)
    return rtn;
  
  status = PQresultStatus( res);
  if( status != PGRES_TUPLES_OK) {
    fprintf( stderr, "popIs failure: %s\n %s\n", PQresStatus( status), PQresultErrorMessage( res));
    PQclear( res);
    return rtn;
  }

  rtn = PQntuples( res);
  if( rtn != 1) {
    PQclear( res);
    return 0;		// more than one row is bad, treat it as though it didn't find any
  }

  // CREATE TYPE rmt.isType AS ( isuser text, isip inet, isport int, fn text, xsize int, ysize int, contrast int, wval int, x int, y int, width int, height int);

  inf->user      = PQgetvalue( res, 0, PQfnumber( res, "isuser"));
  inf->esaf      = atoi(PQgetvalue( res, 0, PQfnumber( res, "isesaf")));
  inf->ip        = PQgetvalue( res, 0, PQfnumber( res, "isip"));
  inf->port      = atoi(PQgetvalue( res, 0, PQfnumber( res, "isport")));
  inf->fn        = PQgetvalue( res, 0, PQfnumber( res, "fn"));
  inf->xsize     = atoi(PQgetvalue( res, 0, PQfnumber( res, "xsize")));
  inf->ysize     = atoi(PQgetvalue( res, 0, PQfnumber( res, "ysize")));
  inf->contrast  = atoi(PQgetvalue( res, 0, PQfnumber( res, "contrast")));
  inf->wval      = atoi(PQgetvalue( res, 0, PQfnumber( res, "wval")));
  inf->x         = atoi(PQgetvalue( res, 0, PQfnumber( res, "x")));
  inf->y         = atoi(PQgetvalue( res, 0, PQfnumber( res, "y")));
  inf->width     = atoi(PQgetvalue( res, 0, PQfnumber( res, "width")));
  inf->height    = atoi(PQgetvalue( res, 0, PQfnumber( res, "height")));



  //
  // free memory from the previous call, if appropriate
  //
  if( isResult != NULL)
    PQclear( isResult);

  //
  // save this so we don't have to allocate memory for all those strings we are pointing to
  //
  isResult = res;

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
