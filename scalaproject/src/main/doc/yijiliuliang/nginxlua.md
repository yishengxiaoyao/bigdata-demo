# Nginx + Lua 页面动态生成
Nginx+lua 脚本做页面动态生成的工作,每次请求过来,优先从Nginx本地缓存中提取各种数据,结合页面模板,生成需要的页面,如果Nginx本地缓存过期了,
从Redis中去拉取数据,更新到Nginx本地,如果Redis中也被LRU算法清理掉了,从Nginx发送Http请求到后端的服务中拉取数据,数据生产服务中,
现在本地Tomcat里的JVM堆缓存(Ehcache)中找,如果也被LRU清理掉了,就重新发送请求到源头的服务中去拉取数据,然后再次更新Tomcat堆内存缓存+Redis缓存,
并返回数据给Nginx缓存到本地。

