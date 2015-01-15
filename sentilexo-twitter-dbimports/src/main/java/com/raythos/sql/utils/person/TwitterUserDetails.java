/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sql.utils.person;

import com.raythos.sql.utils.person.UserDetailsStringParser;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author yanni
 */
public class TwitterUserDetails {
        Long id;
        String name;
        String screenName;
        String location;         
        String description;
        String profileImageUrl;
        String url; 
        boolean isProtected;
                      
        int followersCount;
        int friendsCount;
        Date createdAt;
           
        int favouritesCount;
        int statusesCount;
        boolean isVerified;
        int listedCount;
    private static final String df = "EEE MMM dd hh:mm:ss z yyyy";
    public TwitterUserDetails(){} 
    
    
    
    public void parseFromSerialisedTWitter4JObject(String s) throws Exception {
      SimpleDateFormat sdf = new SimpleDateFormat(df);
           HashMap values = UserDetailsStringParser.parseFromSerialisedTWitter4JObject(s);
           
           id  = Long.parseLong((String)values.get("id"));
           name = (String)values.get("name");
           screenName = (String)values.get("screenName");
          
           location = (String)values.get("location");
           description = (String)values.get("description");
           profileImageUrl = (String)values.get("profileImageUrl");
           url = (String)values.get("url");
           isProtected = Boolean.parseBoolean((String)values.get("isProtected"));
           followersCount =  Integer.parseInt((String) values.get("followersCount"));
           friendsCount   = Integer.parseInt((String)values.get("friendsCount"));
           createdAt = sdf.parse((String)values.get("createdAt"));
           favouritesCount = Integer.parseInt((String)values.get("favouritesCount")); 
           statusesCount = Integer.parseInt((String)values.get("statusesCount"));
           isVerified= Boolean.parseBoolean((String)values.get("isVerified")); 
           listedCount = Integer.parseInt((String)values.get("listedCount"));

    }
    
  /*  public void parseFromTWitter4JObject(UserDetails ud){
        
    }
 */   
    
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreeName(String ScreenName) {
        this.screenName = ScreenName;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getProfileImageUrl() {
        return profileImageUrl;
    }

    public void setProfileImageUrl(String profileImageUrl) {
        this.profileImageUrl = profileImageUrl;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean isProtected() {
        return isProtected;
    }

    public void setIsProtected(boolean isProtected) {
        this.isProtected = isProtected;
    }

    public int getFollowersCount() {
        return followersCount;
    }

    public void setFollowersCount(int followersCount) {
        this.followersCount = followersCount;
    }

    public int getFriendsCount() {
        return friendsCount;
    }

    public void setFriendsCount(int friendsCount) {
        this.friendsCount = friendsCount;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public int getFavouritesCount() {
        return favouritesCount;
    }

    public void setFavouritesCount(int favouritesCount) {
        this.favouritesCount = favouritesCount;
    }

    public int getStatusesCount() {
        return statusesCount;
    }

    public void setStatusesCount(int statusesCount) {
        this.statusesCount = statusesCount;
    }

    public boolean isVerified() {
        return isVerified;
    }

    public void setIsVerified(boolean isVerified) {
        this.isVerified = isVerified;
    }

    public int getListedCount() {
        return listedCount;
    }

    public void setListedCount(int listedCount) {
        this.listedCount = listedCount;
    }
        
        
        
        
}
