package com.edu.bigdata.transform.common;

public enum KpiType {
    NEW_INSTALL_USER("new_install_user"),
    BROWSER_NEW_INSTALL_USER("browser_new_install_user");

    public String name;

    KpiType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
