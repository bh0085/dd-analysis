
import firebase_admin
from firebase_admin import storage
from firebase_admin import credentials
cred = credentials.Certificate('/home/ben_coolship_io/.ssh/jwein-206823-firebase-adminsdk-askpb-7709f1ff8f.json')
firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://jwein-206823.firebaseio.com/',
    'storageBucket': 'slides.dna-microscopy.org'
})

from firebase_admin import db
root = db.reference("datasets/all_v2")