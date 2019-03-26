God.watch do |w|
  w.name = "run-blat"
  w.start = "/home/ben_coolship_io/dd-alignment-server/scripts/run-blat.sh"
  w.keepalive
  w.log = "/home/ben_coolship_io/dd-alignment-server/scripts/logs/run-blat.log"
end


God.watch do |w|
  w.name = "worker"
  w.start = "/home/ben_coolship_io/dd-alignment-server/worker_app.py"
  w.keepalive
  w.log = "/home/ben_coolship_io/dd-alignment-server/scripts/logs/worker_app.log"
end


