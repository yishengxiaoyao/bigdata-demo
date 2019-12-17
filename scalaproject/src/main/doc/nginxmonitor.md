# 监控Nginx
## 启动nginx_exporter
```shell script

docker run -d \
--net=host \
--restart=always \
--name=nginx-exporter \
-v /var/log/nginx:/var/log/nginx \
-e NGINX_ACCESS_LOGS=/var/log/nginx/access.log \
-e NGINX_STATUS_URI=http://127.0.0.1:8888/nginx_status \
quay.io/rebuy/nginx-exporter:v1.1.0
```
## 配置nginx
### 修改nginx.conf
在http模块中添加内容如下:
```
log_format mtail '$host $remote_addr - $remote_user [$time_local] '
                 '"$request" $status $body_bytes_sent $request_time '
                 '"$http_referer" "$http_user_agent" "$content_type" "$upstream_cache_status"';
access_log /var/log/nginx/access.log mtail;
```
### 添加配置文件
在conf.d文件夹下，或者其他文件夹下，需要添加新的conf文件，文件内容如下:
```
server {
    listen 8888;
    location /nginx_status {
      stub_status on;
      access_log  off;
      allow       127.0.0.1;
      deny        all;
    }
}
```
### 验证配置文件的准确性
```shell script
nginx -t  # 校验配置文件是否有问题
nginx -s reload # 重新加载配置
```
### 配置prometheus
在配置文件prometheus.yml中，添加相应的job。
```yml
- job_name: job-test
    static_configs:
    - targets: ['localhost:9397']
```

## 校验
### prometheus状态
在prometheus页面中,Status->Targets，查看实例是否为UP。
### 查看nginx状态
```shell script
curl localhost:8888/nginx_status # 查看nginx的状态
curl localhost:9397/metrics|wc -l # 查看nginx metrics的输出
```
### Grafana查看
在grafana中查看相应的dashboard，然后看到相应的数据。
## 参考文献
[nginx-exporter](https://github.com/rebuy-de/nginx-exporter)