/*
 * Copyright 2014 (c) Raythos Interactive Ltd.  http://www.raythos.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.raythos.sentilexo.twitter.domain;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 * @author yanni
 */
public class QueryResultItem implements Serializable {
    private static final long serialVersionUID = 1L;
    private long statusId;
   
    private String query;
    private String queryName;
    private List<String> relevantQueryTerms;

    private List<Long > contributors;
    
    private Date createdAt;
    private String json;
    private String lang;
  
    
    private long  currentUserRetweetId;
    
  

    private String inReplyToScreenName;
    private long inReplyToStatusId;
    private long  inReplyToUserId;
  
    private double latitude;
    private double longitude;

  
    private String source;
    private String text;
    private boolean trucated;
    
    private boolean possiblySensitive;

    
    private boolean retweet;
    private int retweetCount;
    private long  retweetStatusId;
    private boolean retweeted;
    private boolean retweetedByMe;
    private String retweetedText;
    
    private int favoriteCount;
    private boolean favourited;
    
    private String place;
    
    private List<String> hashtags;
    private List<String> urls;
    private List<String> scopes;
    private Map<String, Long> mentions;

    private String screenName;
    private long userId;
    private String userLocation;
    private String userName;
    private String userDescription;
    private String userUrl;
    private boolean userIsProtected;
    private int userFollowersCount;
    private Date userCreatedAt;
    private int userFriendsCount;
    private int userListedCount;
    private int userStatusesCount;
    private int userFavoritesCount;

    public QueryResultItem() {
    }
    
    

    public QueryResultItem(long statusId) {
        this.statusId = statusId;
    }

    public long getStatusId() {
        return statusId;
    }

    public void setStatusId(long statusId) {
        this.statusId = statusId;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public List<String> getRelevantQueryTerms() {
        return relevantQueryTerms;
    }

    public void setRelevantQueryTerms(List<String> relevantQueryTerms) {
        this.relevantQueryTerms = relevantQueryTerms;
    }

    public List<Long> getContributors() {
        return contributors;
    }

    public void setContributors(List<Long> contributors) {
        this.contributors = contributors;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public long getCurrentUserRetweetId() {
        return currentUserRetweetId;
    }

    public void setCurrentUserRetweetId(long currentUserRetweetId) {
        this.currentUserRetweetId = currentUserRetweetId;
    }

    public String getInReplyToScreenName() {
        return inReplyToScreenName;
    }

    public void setInReplyToScreenName(String inReplyToScreenName) {
        this.inReplyToScreenName = inReplyToScreenName;
    }

    public long getInReplyToStatusId() {
        return inReplyToStatusId;
    }

    public void setInReplyToStatusId(long inReplyToStatusId) {
        this.inReplyToStatusId = inReplyToStatusId;
    }

    public long getInReplyToUserId() {
        return inReplyToUserId;
    }

    public void setInReplyToUserId(long inReplyToUserId) {
        this.inReplyToUserId = inReplyToUserId;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public boolean isTrucated() {
        return trucated;
    }

    public void setTrucated(boolean trucated) {
        this.trucated = trucated;
    }

    public boolean isPossiblySensitive() {
        return possiblySensitive;
    }

    public void setPossiblySensitive(boolean possiblySensitive) {
        this.possiblySensitive = possiblySensitive;
    }

    public boolean isRetweet() {
        return retweet;
    }

    public void setRetweet(boolean retweet) {
        this.retweet = retweet;
    }

    public int getRetweetCount() {
        return retweetCount;
    }

    public void setRetweetCount(int retweetCount) {
        this.retweetCount = retweetCount;
    }

    public long getRetweetStatusId() {
        return retweetStatusId;
    }

    public void setRetweetStatusId(long retweetStatusId) {
        this.retweetStatusId = retweetStatusId;
    }

    public boolean isRetweeted() {
        return retweeted;
    }

    public void setRetweeted(boolean retweeted) {
        this.retweeted = retweeted;
    }

    public boolean isRetweetedByMe() {
        return retweetedByMe;
    }

    public void setRetweetedByMe(boolean retweetedByMe) {
        this.retweetedByMe = retweetedByMe;
    }

    public String getRetweetedText() {
        return retweetedText;
    }

    public void setRetweetedText(String retweetedText) {
        this.retweetedText = retweetedText;
    }

    public int getFavoriteCount() {
        return favoriteCount;
    }

    public void setFavoriteCount(int favoriteCount) {
        this.favoriteCount = favoriteCount;
    }

    public boolean isFavourited() {
        return favourited;
    }

    public void setFavourited(boolean favourited) {
        this.favourited = favourited;
    }

    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }

    public List<String> getUrls() {
        return urls;
    }

    public void setUrls(List<String> urls) {
        this.urls = urls;
    }

    public List<String> getScopes() {
        return scopes;
    }

    public void setScopes(List<String> scopes) {
        this.scopes = scopes;
    }

    public Map<String, Long> getMentions() {
        return mentions;
    }

    public void setMentions(Map<String, Long> mentions) {
        this.mentions = mentions;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getUserLocation() {
        return userLocation;
    }

    public void setUserLocation(String userLocation) {
        this.userLocation = userLocation;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserDescription() {
        return userDescription;
    }

    public void setUserDescription(String userDescription) {
        this.userDescription = userDescription;
    }

    public String getUserUrl() {
        return userUrl;
    }

    public void setUserUrl(String userUrl) {
        this.userUrl = userUrl;
    }

    public boolean isUserIsProtected() {
        return userIsProtected;
    }

    public void setUserIsProtected(boolean userIsProtected) {
        this.userIsProtected = userIsProtected;
    }

    public int getUserFollowersCount() {
        return userFollowersCount;
    }

    public void setUserFollowersCount(int userFollowersCount) {
        this.userFollowersCount = userFollowersCount;
    }

    public Date getUserCreatedAt() {
        return userCreatedAt;
    }

    public void setUserCreatedAt(Date userCreatedAt) {
        this.userCreatedAt = userCreatedAt;
    }

    public int getUserFriendsCount() {
        return userFriendsCount;
    }

    public void setUserFriendsCount(int userFriendsCount) {
        this.userFriendsCount = userFriendsCount;
    }

    public int getUserListedCount() {
        return userListedCount;
    }

    public void setUserListedCount(int userListedCount) {
        this.userListedCount = userListedCount;
    }

    public int getUserStatusesCount() {
        return userStatusesCount;
    }

    public void setUserStatusesCount(int userStatusesCount) {
        this.userStatusesCount = userStatusesCount;
    }

    public int getUserFavoritesCount() {
        return userFavoritesCount;
    }

    public void setUserFavoritesCount(int userFavoritesCount) {
        this.userFavoritesCount = userFavoritesCount;
    }
    
    
    

}

