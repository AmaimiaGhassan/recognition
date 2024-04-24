import os
import face_recognition
import datetime
import db

def encode_faces(connection):
  # load images from the captures directory
  for image in os.listdir('captures'):
    face_image = face_recognition.load_image_file(f"captures/{image}")
    face_encodings = face_recognition.face_encodings(face_image)
    if face_encodings:
      face_encoding = face_encodings[0]
      print(face_encoding)
      timestamp = datetime.datetime.now()
      username = image.split('.')[0]
      db.insert_face(connection, username, face_encoding, timestamp)
      print(f"Encoded {username} face and inserted into the database.")
    else:
      print(f"No faces found in the image {image}.")