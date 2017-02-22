var Redis = require('idredis');

var rClient = new Redis({
  host: '10.1.253.10',
  port: 6379,
  db: 0
});

while (true) {
  rClient.brpop('ISREQEST', 0, function(err, result) {
    if (err) {
      logger.error('Redis blpop failed', {err: err});
      return;
    }
    logger.info('brpop returned', {result: JSON.parse(result)});
  });
}
