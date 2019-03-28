God.watch do |w|
  w.name = "simple"
  w.start = "/home/ben_coolship_io/bin/gfServer -tileSize=8 -minMatch=1 start 0.0.0.0 8080 master.2bit"
  w.dir = "/data/dd-analysis/master_blat/"
  w.keepalive
  w.log = "/home/ben_coolship_io/dd-alignment-server/.god-logs/gf.log"
end