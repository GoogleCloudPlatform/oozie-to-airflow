CONTENTS OF THIS FILE
---------------------

* Introduction
* Requirements
* Recommended modules
* Installation
* Monitoring
* Configuration
* Troubleshooting
* FAQ


INTRODUCTION
------------

This is a  package for installing and configuring airflow v2.2.5 on a 
centos machine with celery executor.
The package performs the installation of postgresql 9.6, rabbitmq and airflow.
It also configures the airflow database and creates daemon services needed.



* For more information on airflow with celery executor, please visit:
  https://insaid.medium.com/set-up-celery-flower-and-rabbitmq-for-airflow-1ed552fe131f
  https://airflow.apache.org/docs/stable/installation.html
  
* For more documentation on rabbitmq, please visit the following link:
  https://www.rabbitmq.com/

* For more information on celery, please visit the following link:
  https://docs.celeryproject.org/en/latest/userguide/index.html
  
REQUIREMENTS
------------

This module requires the following conditions to be met on target machine:

* [Python 2 or 3(recommanded)](https://www.python.org/downloads/)
* [Ssh with root user allowed](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/v2v_guide/preparation_before_the_p2v_migration-enable_root_login_over_ssh)
* Internet connection

The module has been tested on CentOS 7.8.2003 but should work on any other version.


INSTALLATION
------------

1- First, update the inventory.yml file with the target machine ip address.
```
  hosts: 
    HOST_NAME:  
       ansible_user: root
       ansible_host: IP_ADDRESS

```

2- Update group vars according to your needs
  
3-  Make sure your Ansible machine has root access via ssh to the target machines.
  For documentation on how to do this, please visit the following link:
  https://docs.ansible.com/ansible/latest/intro_getting_started.html#intro-getting-started
  
4- Then run the playbook:
```
  ansible-playbook -i inventory.yml install_airflow.yml
```

MONITORING 
------------

Here is a list of the services that are installed and running on the target machine:
- posgresql-9.6.service
- rabbitmq-server.service
- airflow-webserver.service
- airflow-scheduler.service
- airflow-celery-flower.service

You can start or stop some service with these commands
```
  systemctl start <service_name>
  systemctl stop <service_name> 
```


You can monitor airflow dags on airflow web ui running on port {{ airfow.webserver.port }} 
configured in the group vars file.
```
  http://IP_ADDRESS:{{ airfow.webserver.port }}
```

You can monitor celery workers on celery flower running on port  5555
Here is the format of the url:
```
  http://IP_ADDRESS:5555
```

Rabbitmq management console is available on port 15672
```
  http://IP_ADDRESS:15672
```

#### NB: Credentials for rabbitmq, airflow and flower are configurable in the group vars file. 

 
CONFIGURATION
-------------

* To configure airflow, please visit the following link:
  https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html?highlight=configure
  
   

* To configure rabbitmq, please visit the following link:
  https://www.rabbitmq.com/configure.html




TROUBLESHOOTING
---------------

* If some services are not running , please check the journalctl logs.
  For example, if airflow scheduler is not running, please check the airflow logs via this command.
  ```
  journalctl -u airflow-scheduler.service
  ```
  Or restart the airflow scheduler service.
  ```
  systemctl restart airflow-scheduler.service
  ```

FAQ
---

