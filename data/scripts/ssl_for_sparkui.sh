# source: https://www.ibm.com/support/knowledgecenter/en/SS3H8V_1.1.0/com.ibm.izoda.v1r1.azka100/topics/azkic_t_securingwebUIs.htm

echo ">>> Setting up SSL for Spark UI..."
echo "Keystore path : "
read -s  -r KEYSTORE_PATH
echo "Truststore path : "
read -s  -r TRUSTSTORE_PATH
echo "Store password : "
read -s  -r STORE_PASS
echo "Key password : "
read -s  -r KEY_PASS
echo "L : "
read -s -r L
echo "S : "
read -s -r S
echo "C : "
read -s -r C

keytool -genkeypair -keystore "$KEYSTORE_PATH/keystore" -keyalg RSA -alias selfsigned -dname "CN=sparkcert L=$L S=$S C=$C" -storepass "$STORE_PASS" -keypass "$KEY_PASS"

keytool -exportcert -keystore "$KEYSTORE_PATH/keystore" -alias selfsigned -storepass $STORE_PASS -file spark.cer

# note: do not forget to import cert in all nodes
keytool -importcert -keystore "$TRUSTSTORE_PATH/truststore" -alias selfsigned -storepass $STORE_PASS -file spark.cer -noprompt