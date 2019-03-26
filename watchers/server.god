God.watch do |w|
  w.name = "worker"
  w.start = "ruby ./worker.py"
  w.keepalive
end