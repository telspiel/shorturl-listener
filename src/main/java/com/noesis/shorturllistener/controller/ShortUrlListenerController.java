package com.noesis.shorturllistener.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.servlet.view.RedirectView;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.noesis.domain.persistence.NgAnalyticsCurrDate;
import com.noesis.domain.persistence.NgShortUrlChildMapping;
import com.noesis.domain.service.NgAnalyticsCurrDateService;
import com.noesis.domain.service.NgShortUrlChildMappingService;
import com.noesis.domain.service.StaticDataService;
import com.noesis.domain.service.UserService;
import com.noesis.shorturllistener.kafka.MessageProducer;

import eu.bitwalker.useragentutils.UserAgent;

@Controller
public class ShortUrlListenerController {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private MessageProducer producer;

	/*@Autowired
	private ShortUrlService shortUrlServive;*/

	@Autowired
	private StaticDataService staticDataService;

	
	@Autowired
	private NgShortUrlChildMappingService childShortUrlService;
	
	@Autowired
	private UserService userService;

	@Autowired
	private NgAnalyticsCurrDateService ngAnalyticsCurrDateService;
	
	@Value("${kafka.topic.short.url.mis}")
	private String shortUrlMisTopicName;

	@GetMapping(path = "/")
	public String imUpAndRunning() {
		return "{healthy:true}";
	}
	
	@GetMapping(value = "/{uniqueId}")
	public RedirectView redirectUrl(@PathVariable String uniqueId, HttpServletRequest request,
			HttpServletResponse response)  {
		try {
			logger.info("Received shortened url to redirect: " + uniqueId);
			String userIpAdd = request.getHeader("real-ip");
			String userAgentString = request.getHeader("user-agent");

			String userCountry = request.getHeader("Remote-Country");
			
			//Added by Ashish Sir(IF block with condition rest are same)
			if (!userAgentString.contains("Googlebot") && !userAgentString.contains("Google-PageRenderer") &&
					!userAgentString.contains("developers.google.com") &&
					!userAgentString.contains("bot.html") && userCountry != null) {
				String userRegion = request.getHeader("Remote-Region");
				String userCity = request.getHeader("Remote-City");
				String userZip = request.getHeader("Remote-ZIP");
				UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
				logger.info("Received header in request {} : ", request.getHeaderNames());
				Map<String, String> childShortUrlDetailsMap = childShortUrlService.getChildShortUrlDetailsFromKey(uniqueId);
				if (childShortUrlDetailsMap != null) {
					String redirectUrlString = childShortUrlDetailsMap.get("longUrl");
					String callbackUrl = childShortUrlDetailsMap.get("callbackUrl");
					String userId = childShortUrlDetailsMap.get("userId");
					String isDynamic = childShortUrlDetailsMap.get("isDynamic");
					String mobileNumber = childShortUrlDetailsMap.get("mobileNumber");
					String campaignName = childShortUrlDetailsMap.get("campaignName");
					String childShortUrlId = childShortUrlDetailsMap.get("childShortUrlId");
					String parentShortUrl = childShortUrlDetailsMap.get("parentShortUrl");
					String parentShortUrlId = childShortUrlDetailsMap.get("parentShortUrlId");
					String campaignId = childShortUrlDetailsMap.get("campaignId");
					String campaignDate = childShortUrlDetailsMap.get("campaignDate");
					
					String originalLongUrl = redirectUrlString;
					logger.info("Original URL: " + redirectUrlString);
					redirectUrlString = populateUrlParameters(redirectUrlString, mobileNumber, campaignName);
					logger.info("Callback URL is : {}", callbackUrl);
					RedirectView redirectView = new RedirectView();
					redirectView.setUrl(redirectUrlString);
					
					// NgUser ngUser =
					// userService.getUser(Integer.parseInt(userId));
					// NgShortUrl ngShortUrl =
					// shortUrlServive.getShortUrlForUserAndUniqueKey(ngUser.getId(),
					// uniqueId);
					if (callbackUrl != null && callbackUrl.length() > 0 && !callbackUrl.equalsIgnoreCase("null")) {
						callbackUrl = populateUrlParameters(callbackUrl, mobileNumber, campaignName);

						URL obj = new URL(callbackUrl);
						HttpURLConnection con = (HttpURLConnection) obj.openConnection();
						con.setRequestMethod("GET");
						int responseCode = con.getResponseCode();

						BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
						String inputLine;
						StringBuffer callbackUrlResponse = new StringBuffer();
						while ((inputLine = in.readLine()) != null) {
							callbackUrlResponse.append(inputLine);
						}
						in.close();
						logger.info("Callback {} Url Response is {} ", callbackUrl, callbackUrlResponse.toString());
					}
					
					logger.error("Child short url id: {}", childShortUrlId);
					if(childShortUrlId != null && childShortUrlId != "0" && childShortUrlId.length() > 0){
						NgShortUrlChildMapping ngChildShortUrlMapping = childShortUrlService.findById(Integer.parseInt(childShortUrlId));
						if(ngChildShortUrlMapping != null){
							parentShortUrl = ngChildShortUrlMapping.getParentShortUrl();
							parentShortUrlId = ""+ngChildShortUrlMapping.getParentShortUrlId();
							campaignId = ""+ngChildShortUrlMapping.getCampaignId();
						}
					}
					
					NgAnalyticsCurrDate ngAnalysisCurrDate = new NgAnalyticsCurrDate();
					ngAnalysisCurrDate.setBrowserDetails(userAgent.getBrowser().getName());
					// ngAnalysisCurrDate.setCampaignId(1);
					ngAnalysisCurrDate.setCampaignName(campaignName);
					ngAnalysisCurrDate.setChildShortUrl(uniqueId);
					ngAnalysisCurrDate.setCompleteHeader(userAgentString);
					if(campaignDate != null){
						try{
							SimpleDateFormat parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
							Date date = parser.parse(campaignDate);
							ngAnalysisCurrDate.setCreatedDate(date);
						}catch (Exception e) {
							logger.error("Error whille parsing campaign date {}. Setting todays date. ", campaignDate);
							ngAnalysisCurrDate.setCreatedDate(new Date());
						}
					}else {
						ngAnalysisCurrDate.setCreatedDate(new Date());
					}
					
					ngAnalysisCurrDate.setCurrDate(new Date());
					ngAnalysisCurrDate.setDeviceDetails(userAgent.getOperatingSystem().getManufacturer().getName());
					if (mobileNumber.length() == 10) {
						ngAnalysisCurrDate.setMobileNumber("91" + mobileNumber);
					} else {
						ngAnalysisCurrDate.setMobileNumber(mobileNumber);
					}
					
					ngAnalysisCurrDate.setOperatingSystem(userAgent.getOperatingSystem().getName());
					ngAnalysisCurrDate.setParentShortUrl(parentShortUrl);
					
					if(parentShortUrlId != null)
						ngAnalysisCurrDate.setParentShortUrlId(Integer.parseInt(parentShortUrlId));
					
					
					ngAnalysisCurrDate.setUpdatedDate(new Date());
					
					if(userId != null)
						ngAnalysisCurrDate.setUserId(Integer.parseInt(userId));
					
					ngAnalysisCurrDate.setUserIpAddress(userIpAdd);
					
					if(campaignId != null)
						ngAnalysisCurrDate.setCampaignId(Integer.parseInt(campaignId));
					
					ngAnalysisCurrDate.setCountry(userCountry);
					ngAnalysisCurrDate.setRegion(userRegion);
					ngAnalysisCurrDate.setCity(userCity);
					
					if(userZip != null){
						logger.error("User zip is : ",userZip);
						if(userZip.matches("\\d+"))
							ngAnalysisCurrDate.setZip(Integer.parseInt(userZip));
					}
					
					ngAnalysisCurrDate.setLongUrl(originalLongUrl);

					ngAnalyticsCurrDateService.save(ngAnalysisCurrDate);
					
					// Convert dlr object into json string
					/*
					 * try { String analyticsObjectAsJsonString =
					 * objectMapper.writeValueAsString(ngAnalysisCurrDate); // send
					 * dlr json string into kafka all-received-dlr queue
					 * producer.send(shortUrlMisTopicName,
					 * analyticsObjectAsJsonString);
					 * logger.info("Dlr sent to 'alldlr' kafka queue : " +
					 * analyticsObjectAsJsonString); } catch
					 * (JsonProcessingException e) {
					 * logger.error("Error while converting dlr object into json.");
					 * e.printStackTrace(); } catch (Exception e) {
					 * logger.error("Error while converting dlr object into json.");
					 * e.printStackTrace(); }
					 */
					return redirectView;
					
				}
				
				
			}

			return new RedirectView("/error.jsp");

		}catch (Exception e) {
			logger.error("Error while redirecting to long url");
			e.printStackTrace();
		}
		return new RedirectView("/error.jsp");
	}

	private String populateUrlParameters(String redirectUrlString, String mobileNumber, String campaignName) {
		if(redirectUrlString.contains("%MOBILE%")) {
			redirectUrlString = redirectUrlString.replace("%MOBILE%", mobileNumber);
		}
		if(redirectUrlString.contains("%CAMPAIGN_NAME%")) {
			redirectUrlString = redirectUrlString.replace("%CAMPAIGN_NAME%", campaignName);
		}
		
		// Get operator and circle info:
		Integer circleId = 999;
		Integer carrierId = 999;
		String numSeries = null;
		if(mobileNumber != null && mobileNumber.startsWith("91")) {
			numSeries = mobileNumber.substring(2, 7);
		}else {
			numSeries = mobileNumber.substring(0, 5);
		}
		String carrierCircleInfo = staticDataService.getCarrierCircleForSeries(numSeries);
		String carrierCircleData[] = carrierCircleInfo.split("#");
		logger.error("carrierID is ", carrierCircleData[0]);
		logger.error("circleID is ", carrierCircleData[1]);
		if(carrierCircleData[0] != null){
			carrierId = Integer.parseInt(carrierCircleData[0]);
		}
		if(carrierCircleData[1] != null){
			circleId = Integer.parseInt(carrierCircleData[1]);
		}
		
		String carrierName = staticDataService.getCarrierNameById(carrierId);
		String circleName = staticDataService.getCircleNameById(circleId);
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMyyHHmmss");
		String receiveTime = dateFormat.format(new Date());
		
		if(redirectUrlString.contains("%CIRCLE%")) {
			redirectUrlString = redirectUrlString.replace("%CIRCLE%", circleName);
		}
		if(redirectUrlString.contains("%OPERATOR%")) {
			redirectUrlString = redirectUrlString.replace("%OPERATOR%", carrierName);
		}
		if(redirectUrlString.contains("%RECTIME%")) {
			redirectUrlString = redirectUrlString.replace("%RECTIME%", receiveTime);
		}
		return redirectUrlString;
	}

	public static void main(String[] args) {
		String campaignDate = "Thu Jun 18 20:56:02 EDT 2009";
		SimpleDateFormat parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
	    Date date;
		try {
			date = parser.parse(campaignDate);
			System.out.println(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
}
