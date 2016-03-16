Vagrant.configure("2") do |config|
  config.vm.provider "virtualbox" do |v|
    host = RbConfig::CONFIG['host_os']

    # Give VM 1/4 system memory & access to all cpu cores on the host
    if host =~ /darwin/
      cpus = `sysctl -n hw.ncpu`.to_i
      mem = `sysctl -n hw.memsize`.to_i / 1024 / 1024 / 4
    elsif host =~ /linux/
      cpus = `nproc`.to_i
      mem = `grep 'MemTotal' /proc/meminfo | sed -e 's/MemTotal://' -e 's/ kB//'`.to_i / 1024 / 4
    else # windows, probably
      cpus = `cmd /c "echo %NUMBER_OF_PROCESSORS%"`.to_i
      mem = 1024 # it's no simple task to get the memory size from the command line!
    end

    v.customize ["modifyvm", :id, "--memory", mem]
    v.customize ["modifyvm", :id, "--cpus", cpus]
  end

  config.vm.define "dev" do |dev|
    dev.ssh.insert_key = false
    dev.vm.box = "ubuntu/trusty64"
    dev.vm.network :forwarded_port, host: 5000, guest: 5000
    dev.vm.network :forwarded_port, host: 5432, guest: 5432
    dev.vm.provision "ansible" do |ansible|
      ansible.playbook = "ansible/dev.yml"
    end
  end
end