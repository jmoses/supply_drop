module SupplyDrop
  module Plugin
    def update
      if fetch(:puppet_git_url, nil)
        git
      else
        rsync
      end
    end

    def rsync
      SupplyDrop::Util.thread_pool_size = puppet_parallel_rsync_pool_size
      servers = SupplyDrop::Util.optionally_async(find_servers_for_task(current_task), puppet_parallel_rsync)
      overrides = {}
      overrides[:user] = fetch(:user, ENV['USER'])
      overrides[:port] = fetch(:port) if exists?(:port)
      failed_servers = servers.map do |server|
        rsync_cmd = SupplyDrop::Rsync.command(
          puppet_source,
          SupplyDrop::Rsync.remote_address(server.user || fetch(:user, ENV['USER']), server.host, puppet_destination),
          :delete => true,
          :excludes => puppet_excludes,
          :ssh => ssh_options.merge(server.options[:ssh_options]||{}).merge(overrides)
        )
        logger.debug rsync_cmd
        server.host unless system rsync_cmd
      end.compact

      raise "rsync failed on #{failed_servers.join(',')}" if failed_servers.any?
    end

    def git
      git_url = fetch(:puppet_git_url, nil)
      git_user = fetch(:puppet_git_user, fetch(:user))

      raise ArgumentError.new("puppet_git_url not defined") unless git_url

      run "if grep '#{fetch :puppet_git_key}' ~#{git_user}/.ssh/known_hosts ; then true ; else #{sudo as: git_user} echo '#{fetch :puppet_git_key}' >> ~#{git_user}/.ssh/known_hosts ; fi"

      clone_cmd = "#{sudo as: git_user} git clone #{git_url} #{fetch(:puppet_destination)}"
      update_cmd = "cd #{fetch(:puppet_destination)} && #{sudo as: git_user} git fetch"
      update_cmd +=  " && #{sudo as: git_user} git reset --hard origin/HEAD"

      if git_user != fetch(:user)
        run "chown -R #{git_user} #{fetch :puppet_destination}"
      end

      run "if [ ! -d #{fetch :puppet_destination}/.git ]; then #{clone_cmd}; else #{update_cmd}; fi"
    end


    def prepare
      run "mkdir -p #{puppet_destination}"
      run "#{sudo} chown -R $USER: #{puppet_destination}"
    end

    def noop
      puppet(:noop)
    end

    def apply
      puppet(:apply)
    end

    def lock
      if should_lock?
        run <<-CHECK_LOCK
if [ -f #{puppet_lock_file} ]; then
    stat -c "#{red_text("Puppet in progress, #{puppet_lock_file} owned by %U since %x")}" #{puppet_lock_file} >&2;
    exit 1;
fi
        CHECK_LOCK

        run "touch #{puppet_lock_file}"
      end
    end

    def unlock
      run "#{sudo} rm -f #{puppet_lock_file}; true" if should_lock?
    end

    private

    def should_lock?
      puppet_lock_file && !ENV['NO_PUPPET_LOCK']
    end

    def puppet(command = :noop)
      puppet_cmd = "cd #{puppet_destination} && #{sudo_cmd} #{puppet_command} --modulepath=#{puppet_lib} #{puppet_parameters}"
      flag = command == :noop ? '--noop' : ''

      writer = if puppet_stream_output
                 SupplyDrop::Writer::Streaming.new(logger)
               else
                 SupplyDrop::Writer::Batched.new(logger)
               end

      writer = SupplyDrop::Writer::File.new(writer, puppet_write_to_file) unless puppet_write_to_file.nil?

      begin
        run "#{puppet_cmd} #{flag}" do |channel, stream, data|
          writer.collect_output(channel[:host], data)
        end
        logger.debug "Puppet #{command} complete."
      ensure
        writer.all_output_collected
      end
    end

    def red_text(text)
      "\033[0;31m#{text}\033[0m"
    end

    def sudo_cmd
      if fetch(:use_sudo, true)
        sudo(:as => puppet_runner)
      else
        logger.info "NOTICE: puppet_runner configuration invalid when use_sudo is false, ignoring..." unless puppet_runner.nil?
        ''
      end
    end
  end
end
