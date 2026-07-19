import subprocess

with open('debug_out.txt', 'w', encoding='utf-8') as f:
    proc = subprocess.Popen(
        ["go", "run", "main.go", "-topic", "Football", "-limit", "5"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    for line in proc.stdout:
        f.write(line.decode('utf-8', errors='replace').replace('\r', '[CR]'))
