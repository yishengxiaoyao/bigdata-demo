# AWS CloudFront 清缓存

## 故障介绍
为了提高用户的体验度、页面的流畅度，为前段做了CDN。为了提供工作效率，为前段同学提供自动化部署的工具，
由于CDN是有缓存的功能，如果禁止缓存的功能，CDN就起不到作用，所以在每次上线的时候，需要将CDN中的缓存
清除。
如果想要在服务器上清除AWS CloudFront中的缓存，需要在服务器上安装aws的客户端，然后配置相应的key，保证
这个账号具有执行CloudFront功能的权限，否则会输出没有权限的错误。
在这些准备工作都已经做完，然后执行AWS CloudFront的命令:
```shell script
aws cloudfront create-invalidation --distribution-id cloudfront的编号 --paths 清除缓存的路径
```
在执行的过程中，输出错误信息:
```text
AWS CLI support for this service is only available in a preview stage.

However, if you'd like to use the "aws cloudfront" commands with the
AWS CLI, you can enable this service by adding the following to your CLI
config file.
```
## 解决方法
### 1.修改配置文件
```shell script
cd ~/.aws/
vi config
# add preview to config
往config文件中添加内容如下:
[preview]
cloudfront=true
```
### 2.执行命令
```shell script
aws configure set preview.cloudfront true
```
在执行往上面的命令之后，程序会自动往配置文件中添加方法1中的内容，这个命令执行一次就行。
如果使用常见的自动化部署工具，例如jenkins，可以只需要缓存的命令，先登录到服务器执行添加配置的命令，
这样就不会输出这个错误，如果使用CircleCI，就需要将两个命令放在一起执行，需要将修改配置
文件，才能删除AWS CloudFront 缓存。