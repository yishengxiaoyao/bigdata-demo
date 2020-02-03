public final class Method extends Executable{
	public Object invoke(Object obj,Object... args)throw ...{
		//权限检查
		MethodAccessor ma = methodAccessor;
		if (ma == null){
			ma = acquireMethodAccessor();
		}
		return ma.invoke(obj,args);
	}
	
}