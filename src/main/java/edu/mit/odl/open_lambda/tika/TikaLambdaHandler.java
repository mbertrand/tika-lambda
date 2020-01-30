package edu.mit.odl.open_lambda.tika;

import java.io.IOException;
import java.io.InputStream;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.TransformerConfigurationException;
import org.xml.sax.SAXException;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TikaLambdaHandler implements RequestHandler<S3Event, String> {

	private LambdaLogger _logger;

	private void parseObjectText(AmazonS3 s3Client, String bucket, String key, JSONObject courseJSON,
			Map<String, String> fileJSON) {
		/*
		 * Extract text from an S3 object, add attributes from course JSON, and put in
		 * the extracted text bucket.
		 */

		String extractBucket = bucket + "-extracts";

		S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));

		try (InputStream objectData = s3Object.getObjectContent()) {
			JSONObject extractJson = extractText(bucket, key, objectData);
			String courseId = courseJSON.get("department_number") + "." + courseJSON.get("master_course_number");
			extractJson.put("CourseId", courseId);
			extractJson.put("RunId", courseJSON.get("uid"));
			extractJson.put("Title", fileJSON.get("title"));
			extractJson.put("Description", fileJSON.get("description"));

			byte[] extractBytes = extractJson.toJSONString().getBytes(Charset.forName("UTF-8"));
			int extractLength = extractBytes.length;

			ObjectMetadata metaData = new ObjectMetadata();
			metaData.setContentLength(extractLength);

			_logger.log("Saving extract file to S3");
			InputStream inputStream = new ByteArrayInputStream(extractBytes);
			s3Client.putObject(extractBucket, key + ".extract", inputStream, metaData);
		} catch (IOException | TransformerConfigurationException | SAXException e) {
			_logger.log("Exception: " + e.getLocalizedMessage());
			throw new RuntimeException(e);
		}
	}

	public String handleRequest(S3Event s3event, Context context) {
		_logger = context.getLogger();
		_logger.log("Received S3 Event: " + s3event.toJson());

		try {
			S3EventNotificationRecord record = s3event.getRecords().get(0);

			String bucket = record.getS3().getBucket().getName();

			String json_key = URLDecoder.decode(record.getS3().getObject().getKey().replace('+', ' '), "UTF-8");

			AmazonS3 s3Client = new AmazonS3Client();
			S3Object masterJsonObject = s3Client.getObject(new GetObjectRequest(bucket, json_key));
			S3ObjectInputStream s3Stream = masterJsonObject.getObjectContent();
			JSONParser jsonParser = new JSONParser();
			JSONObject s3Json = (JSONObject) jsonParser.parse(new InputStreamReader(s3Stream, "UTF-8"));

			String courseUrl = s3Json.get("url").toString();
			String coursePrefix = courseUrl.substring(courseUrl.lastIndexOf("/") + 1);

			String[] fileSections = { "course_files", "course_foreign_files" };
			for (String section : fileSections) {
				JSONArray courseFiles = (JSONArray) s3Json.get(section);
				Iterator courseFilesIterator = courseFiles.iterator();

				while (courseFilesIterator.hasNext()) {
					Map<String, String> courseFile = ((Map<String, String>) courseFilesIterator.next());
					String file_uid = courseFile.get("uid");
					if (file_uid == null) {
						file_uid = courseFile.get("link");
						file_uid = file_uid.substring(file_uid.lastIndexOf("/") + 1);
					}
					String s3ObjectPrefix = coursePrefix + "/" + file_uid;
					ObjectListing listing = s3Client.listObjects(bucket, s3ObjectPrefix);
					List<S3ObjectSummary> summaries = listing.getObjectSummaries();

					if (!summaries.isEmpty()) {
						S3ObjectSummary summary = summaries.get(0);
						String objectKey = summary.getKey();
						// Ignore files that won't have text in them
						objectKey = URLDecoder.decode(objectKey.replace('+', ' '), "UTF-8");
						String extension = objectKey.substring(objectKey.lastIndexOf(".") + 1).toLowerCase();
						String[] extensions = { "pdf", "doc", "docx", "ppt", "pptx", "xls", "xlsx", "json", "html",
								"htm", "xml", "txt", "ps", "rtf", "srt", "json" };
						boolean containsExtension = Arrays.stream(extensions).anyMatch(extension::equals);

						if (containsExtension) {
							_logger.log("Extract file " + objectKey);
							parseObjectText(s3Client, bucket, objectKey, s3Json, courseFile);
						}
					} else {
						_logger.log("No S3 objects w/prefix " + s3ObjectPrefix + " found.");
					}

				}
			}

		} catch (IOException | ParseException e) {
			_logger.log("Exception: " + e.getLocalizedMessage());
			throw new RuntimeException(e);
		}
		return "Success";
	}

	private JSONObject extractText(String bucket, String key, InputStream objectData)
			throws IOException, TransformerConfigurationException, SAXException {
		/*
		 * Use Tika to extract text from an S3 object
		 */

		_logger.log("Extracting text with Tika");
		String extractedText = "";

		SAXTransformerFactory factory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
		TransformerHandler handler = factory.newTransformerHandler();
		handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "text");
		handler.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
		StringWriter sw = new StringWriter();
		handler.setResult(new StreamResult(sw));
		AutoDetectParser parser = new AutoDetectParser();
		ParseContext parseContext = new ParseContext();
		parseContext.set(Parser.class, parser);

		Tika tika = new Tika();
		Metadata tikaMetadata = new Metadata();
		try {
			parser.parse(objectData, handler, tikaMetadata, parseContext);
			extractedText = sw.toString();
		} catch (TikaException e) {
			_logger.log("TikaException thrown while parsing: " + e.getLocalizedMessage());
			return assembleExceptionResult(bucket, key, e);
		}
		_logger.log("Tika parsing success");
		return assembleExtractionResult(bucket, key, extractedText, tikaMetadata);
	}

	private JSONObject assembleExtractionResult(String bucket, String key, String extractedText,
			Metadata tikaMetadata) {
		/*
		 *  Generate a JSON object containing extracted text and relevant metadata
		 */
		
		JSONObject extractJson = new JSONObject();

		String contentType = tikaMetadata.get("Content-Type");
		contentType = contentType != null ? contentType : "content/unknown";

		String contentLength = tikaMetadata.get("Content-Length");
		contentLength = contentLength != null ? contentLength : "0";

		extractJson.put("Exception", null);
		extractJson.put("FilePath", "s3://" + bucket + "/" + key);
		extractJson.put("Text", extractedText);
		extractJson.put("ContentType", contentType);
		extractJson.put("ContentLength", contentLength);

		JSONObject metadataJson = new JSONObject();

		for (String name : tikaMetadata.names()) {
			String[] elements = tikaMetadata.getValues(name);
			String joined = String.join(", ", elements);
			metadataJson.put(name, joined);
		}

		extractJson.put("Metadata", metadataJson);

		return extractJson;
	}

	private JSONObject assembleExceptionResult(String bucket, String key, Exception e) {
		/*
		 * Generate a JSON object containing info on what went wrong during text extraction.
		 */
		
		JSONObject exceptionJson = new JSONObject();

		exceptionJson.put("Exception", e.getLocalizedMessage());
		exceptionJson.put("FilePath", "s3://" + bucket + "/" + key);
		exceptionJson.put("ContentType", "unknown");
		exceptionJson.put("ContentLength", "0");
		exceptionJson.put("Text", "");

		JSONObject metadataJson = new JSONObject();
		metadataJson.put("resourceName", "s3://" + bucket + "/" + key);

		exceptionJson.put("Metadata", metadataJson);

		return exceptionJson;
	}
}